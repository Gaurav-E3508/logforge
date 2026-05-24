package com.logforge.storage.cold;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.logforge.common.model.LogEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;

/**
 * Manages all cold storage segment files on disk.
 *
 * Responsibilities:
 * - Write batches of old events to new segment files
 * - Search across segment files for a time range query
 * - List all segments for a given service
 * - Delete segments older than 1 year
 */
@Slf4j
@Component
public class ColdStorageManager {

    private final Path         basePath;
    private final ObjectMapper objectMapper;

    private static final long ONE_YEAR_MS = 365L * 24 * 60 * 60 * 1000;

    public ColdStorageManager(
            @Value("${logforge.storage.cold-base-path:./cold-storage}")
            String coldBasePath,
            ObjectMapper objectMapper) throws IOException {

        this.basePath     = Paths.get(coldBasePath);
        this.objectMapper = objectMapper;
        Files.createDirectories(this.basePath);
        log.info("Cold storage initialized at: {}", this.basePath.toAbsolutePath());
    }

    /**
     * Archive a batch of events to a new segment file.
     * Called by the tiering scheduler when events age out of warm storage.
     */
    public void archive(String serviceName, List<LogEvent> events) {
        if (events.isEmpty()) return;

        long fromTs = events.stream()
                .mapToLong(LogEvent::getTimestamp).min().orElse(0);
        long toTs   = events.stream()
                .mapToLong(LogEvent::getTimestamp).max().orElse(0);

        SegmentFile segment = new SegmentFile(
                basePath, serviceName, fromTs, toTs, objectMapper);

        try {
            segment.write(events);
            log.info("Archived {} events for service '{}' to cold storage",
                    events.size(), serviceName);
        } catch (IOException e) {
            log.error("Failed to write cold segment for service '{}': {}",
                    serviceName, e.getMessage());
        }
    }

    /**
     * Search cold storage for events within a time range.
     * Scans all matching segment files in parallel.
     */
    public List<LogEvent> search(String serviceName, long fromTime, long toTime) {
        List<LogEvent> results = new ArrayList<>();

        List<SegmentFile> matchingSegments = findSegments(serviceName, fromTime, toTime);
        log.debug("Searching {} cold segments for service '{}' in range [{}, {}]",
                matchingSegments.size(), serviceName, fromTime, toTime);

        for (SegmentFile segment : matchingSegments) {
            try {
                results.addAll(segment.readInRange(fromTime, toTime));
            } catch (IOException e) {
                log.error("Failed to read cold segment {}: {}",
                        segment.getPath(), e.getMessage());
            }
        }

        return results;
    }

    /**
     * Delete segments older than 1 year.
     * Called by the tiering scheduler during cleanup.
     */
    public void deleteExpiredSegments() {
        long cutoffTime = System.currentTimeMillis() - ONE_YEAR_MS;

        try (Stream<Path> files = Files.list(basePath)) {
            files.filter(p -> p.toString().endsWith(".seg.gz"))
                    .forEach(p -> {
                        try {
                            // Parse toTimestamp from filename
                            String name = p.getFileName().toString();
                            String[] parts = name.replace(".seg.gz", "").split("_");
                            long toTs = Long.parseLong(parts[parts.length - 1]);
                            if (toTs < cutoffTime) {
                                Files.delete(p);
                                log.info("Deleted expired cold segment: {}", name);
                            }
                        } catch (Exception e) {
                            log.warn("Could not process segment file {}: {}",
                                    p, e.getMessage());
                        }
                    });
        } catch (IOException e) {
            log.error("Error scanning cold storage for cleanup: {}", e.getMessage());
        }
    }

    public long getTotalSegmentCount() {
        try (Stream<Path> files = Files.list(basePath)) {
            return files.filter(p -> p.toString().endsWith(".seg.gz")).count();
        } catch (IOException e) {
            return -1;
        }
    }

    private List<SegmentFile> findSegments(String serviceName,
                                           long fromTime, long toTime) {
        List<SegmentFile> matching = new ArrayList<>();
        String servicePrefix = "segment_" +
                serviceName.replaceAll("[^a-zA-Z0-9]", "_");

        try (Stream<Path> files = Files.list(basePath)) {
            files.filter(p -> p.getFileName().toString().startsWith(servicePrefix)
                            && p.toString().endsWith(".seg.gz"))
                    .forEach(p -> {
                        try {
                            String name   = p.getFileName().toString()
                                    .replace(".seg.gz", "");
                            String[] parts = name.split("_");
                            long segFrom  = Long.parseLong(parts[parts.length - 2]);
                            long segTo    = Long.parseLong(parts[parts.length - 1]);

                            // Segment overlaps the requested range
                            if (segTo >= fromTime && segFrom <= toTime) {
                                matching.add(new SegmentFile(
                                        basePath, serviceName, segFrom, segTo, objectMapper));
                            }
                        } catch (Exception e) {
                            log.warn("Skipping unreadable segment file: {}", p);
                        }
                    });
        } catch (IOException e) {
            log.error("Failed to list cold storage files: {}", e.getMessage());
        }
        return matching;
    }
}