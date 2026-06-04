package com.logforge.ingestion.controller;

import com.logforge.common.kafka.KafkaTopics;
import com.logforge.common.model.LogEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * Test-only endpoint to inject log events directly into Kafka.
 * Remove this before going to production.
 */
@RestController
@RequestMapping("/test")
@RequiredArgsConstructor
public class TestController {

    private final KafkaTemplate<String, LogEvent> kafkaTemplate;

    @PostMapping("/event")
    public String sendEvent(@RequestBody LogEvent event) {
        if (event.getEventId() == null) {
            event = LogEvent.builder()
                    .eventId(UUID.randomUUID().toString())
                    .timestamp(event.getTimestamp() > 0
                            ? event.getTimestamp()
                            : System.currentTimeMillis())
                    .level(event.getLevel())
                    .serviceName(event.getServiceName())
                    .hostName(event.getHostName() != null
                            ? event.getHostName() : "test-host")
                    .environment(event.getEnvironment() != null
                            ? event.getEnvironment()
                            : LogEvent.Environment.DEVELOPMENT)
                    .message(event.getMessage())
                    .sourceIp(event.getSourceIp() != null
                            ? event.getSourceIp() : "10.0.1.5")
                    .traceId(event.getTraceId() != null
                            ? event.getTraceId()
                            : "trace-" + UUID.randomUUID().toString().substring(0, 8))
                    .tags(event.getTags())
                    .build();
        }
        kafkaTemplate.send(KafkaTopics.RAW_LOGS, event.getServiceName(), event);
        return "Event sent: " + event.getEventId();
    }

    @PostMapping("/bulk")
    public String sendBulk() {
        String[][] testData = {
                {"payment-service", "INFO",  "Payment processed for orderId=ORD-001"},
                {"payment-service", "ERROR", "Database timeout after 5000ms"},
                {"auth-service",    "ERROR", "Authentication failed for userId=u_991"},
                {"auth-service",    "INFO",  "User login successful userId=u_123"},
                {"order-service",   "WARN",  "Retry attempt 2 of 3 for payment"},
                {"payment-service", "ERROR", "Payment gateway timeout - call failed"},
                {"payment-service", "FATAL", "OutOfMemoryError: heap space exhausted"},
                {"order-service",   "ERROR", "SQL exception: connection refused"},
                {"auth-service",    "ERROR", "JWT token expired for userId=u_456"},
                {"payment-service", "INFO",  "Refund processed for orderId=ORD-009"},
        };

        for (String[] row : testData) {
            LogEvent event = LogEvent.builder()
                    .eventId(UUID.randomUUID().toString())
                    .timestamp(System.currentTimeMillis())
                    .serviceName(row[0])
                    .level(LogEvent.LogLevel.valueOf(row[1]))
                    .message(row[2])
                    .hostName("test-host-01")
                    .environment(LogEvent.Environment.DEVELOPMENT)
                    .sourceIp("10.0.1." + (int)(Math.random() * 255))
                    .traceId("trace-" + UUID.randomUUID().toString().substring(0, 8))
                    .build();

            kafkaTemplate.send(KafkaTopics.RAW_LOGS, event.getServiceName(), event);

            try { Thread.sleep(100); } catch (InterruptedException e) { break; }
        }
        return "Sent 10 test events to Kafka!";
    }
}