package com.logforge.alert.consumer;

import com.logforge.alert.notification.NotificationDispatcher;
import com.logforge.alert.service.AlertManager;
import com.logforge.alert.statemachine.AlertEvent;
import com.logforge.common.kafka.KafkaTopics;
import com.logforge.common.model.LogEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Consumes log events from Kafka and accumulates per-minute metrics.
 * Every minute the scheduler evaluates rules and dispatches alerts.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AlertConsumer {

    private final AlertManager           alertManager;
    private final NotificationDispatcher dispatcher;

    // Accumulate counts per service for the current evaluation window
    private final ConcurrentHashMap<String, AtomicLong> totalCounts
            = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> errorCounts
            = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> fatalCounts
            = new ConcurrentHashMap<>();

    @KafkaListener(
            topics  = KafkaTopics.PROCESSED_LOGS,
            groupId = "alert-engine-group"
    )
    public void onEvent(LogEvent event) {
        String service = event.getServiceName();
        if (service == null) return;

        totalCounts.computeIfAbsent(service, k -> new AtomicLong()).incrementAndGet();

        if (event.getLevel() == LogEvent.LogLevel.ERROR
                || event.getLevel() == LogEvent.LogLevel.FATAL) {
            errorCounts.computeIfAbsent(service, k -> new AtomicLong())
                    .incrementAndGet();
        }
        if (event.getLevel() == LogEvent.LogLevel.FATAL) {
            fatalCounts.computeIfAbsent(service, k -> new AtomicLong())
                    .incrementAndGet();
        }
    }

    /**
     * Every minute: evaluate all rules against accumulated metrics,
     * dispatch any fired alert events, then reset counters.
     */
    @Scheduled(fixedDelayString =
            "${logforge.alert.evaluation-interval-ms:60000}")
    public void evaluateAndReset() {
        if (totalCounts.isEmpty()) return;

        log.debug("Alert evaluation cycle — {} services", totalCounts.size());

        // Snapshot current counts
        Map<String, Long> totalSnapshot = snapshot(totalCounts);
        Map<String, Long> errorSnapshot = snapshot(errorCounts);
        Map<String, Long> fatalSnapshot = snapshot(fatalCounts);

        // Reset counters for next window
        totalCounts.clear();
        errorCounts.clear();
        fatalCounts.clear();

        // Evaluate each service
        totalSnapshot.forEach((service, total) -> {
            long errors = errorSnapshot.getOrDefault(service, 0L);
            long fatals = fatalSnapshot.getOrDefault(service, 0L);
            double errorRate = total > 0 ? (errors * 100.0) / total : 0;

            List<AlertEvent> events = alertManager.evaluate(
                    service, errorRate, fatals, total);

            events.forEach(dispatcher::dispatch);
        });
    }

    private Map<String, Long> snapshot(
            ConcurrentHashMap<String, AtomicLong> map) {
        ConcurrentHashMap<String, Long> snap = new ConcurrentHashMap<>();
        map.forEach((k, v) -> snap.put(k, v.get()));
        return snap;
    }
}