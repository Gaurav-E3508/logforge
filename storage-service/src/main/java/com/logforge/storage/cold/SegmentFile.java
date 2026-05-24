package com.logforge.storage.cold;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.logforge.common.model.LogEvent;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * A single cold storage segment file on disk.
 *
 * REAL LIFE ANALOGY:
 * The hospital basement has filing boxes. Each box (segment) holds
 * records from a specific date range. Records are compressed to save
 * space — you need a few seconds to decompress and read them.
 *
 * FILE FORMAT:
 * - Each segment = one GZIP-compressed file on disk
 * - Contains JSON-serialized LogEvents, one per line (JSONL format)
 * - Filename: segment_{serviceName}_{fromTimestamp}_{toTimestamp}.seg.gz
 *
 * WHY GZIP?
 * Log messages are highly repetitive text — same words, same patterns.
 * GZIP typically achieves 80-90% compression on log data.
 * 1GB of raw logs → ~100-200MB on disk.
 */
@Slf4j
public class SegmentFile {

    private final Path filePath;
    private final ObjectMapper objectMapper;
    private final long fromTimestamp;
    private final long toTimestamp;
    private final String serviceName;

    public SegmentFile(Path basePath, String serviceName,
                       long fromTimestamp, long toTimestamp,
                       ObjectMapper objectMapper) {
        this.serviceName   = serviceName;
        this.fromTimestamp = fromTimestamp;
        this.toTimestamp   = toTimestamp;
        this.objectMapper  = objectMapper;

        // Create segment filename
        String filename = String.format("segment_%s_%d_%d.seg.gz",
                serviceName.replaceAll("[^a-zA-Z0-9]", "_"),
                fromTimestamp, toTimestamp);
        this.filePath = basePath.resolve(filename);
    }

    /**
     * Write a batch of log events to this segment file.
     * Events are GZIP-compressed and written as JSONL (one JSON per line).
     */
    public void write(List<LogEvent> events) throws IOException {
        // Create parent directories if they don't exist
        Files.createDirectories(filePath.getParent());

        try (OutputStream fos    = Files.newOutputStream(filePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);
             GZIPOutputStream gzip = new GZIPOutputStream(fos);
             BufferedWriter writer  = new BufferedWriter(
                     new OutputStreamWriter(gzip))) {

            for (LogEvent event : events) {
                writer.write(objectMapper.writeValueAsString(event));
                writer.newLine();
            }

            log.info("Wrote {} events to cold segment: {} ({} KB estimated)",
                    events.size(), filePath.getFileName(),
                    events.size() * 200 / 1024); // rough size estimate
        }
    }

    /**
     * Read all events from this segment file.
     * Decompresses and deserializes JSONL back into LogEvent objects.
     */
    public List<LogEvent> read() throws IOException {
        List<LogEvent> events = new ArrayList<>();

        if (!Files.exists(filePath)) {
            log.warn("Segment file not found: {}", filePath);
            return events;
        }

        try (InputStream fis     = Files.newInputStream(filePath);
             GZIPInputStream gzip = new GZIPInputStream(fis);
             BufferedReader reader = new BufferedReader(
                     new InputStreamReader(gzip))) {

            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isBlank()) {
                    events.add(objectMapper.readValue(line, LogEvent.class));
                }
            }
        }

        log.debug("Read {} events from cold segment: {}",
                events.size(), filePath.getFileName());
        return events;
    }

    /**
     * Read only events within a time range — avoids loading the entire segment.
     */
    public List<LogEvent> readInRange(long fromTime, long toTime) throws IOException {
        // Quick check — if segment doesn't overlap the range at all, skip it
        if (toTimestamp < fromTime || fromTimestamp > toTime) {
            return new ArrayList<>();
        }

        return read().stream()
                .filter(e -> e.getTimestamp() >= fromTime
                        && e.getTimestamp() <= toTime)
                .toList();
    }

    public boolean exists()       { return Files.exists(filePath); }
    public Path    getPath()      { return filePath; }
    public long    getFromTime()  { return fromTimestamp; }
    public long    getToTime()    { return toTimestamp; }
    public String  getService()   { return serviceName; }
}