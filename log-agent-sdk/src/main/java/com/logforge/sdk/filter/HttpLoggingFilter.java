package com.logforge.sdk.filter;

import com.logforge.common.model.LogEvent;
import com.logforge.sdk.config.LogAgentProperties;
import com.logforge.sdk.service.LogCaptureService;
import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.UUID;

/**
 * Intercepts every HTTP request passing through the service.
 *
 * REAL LIFE ANALOGY:
 * Think of this as a security camera at the restaurant door.
 * Every person who walks in and out is recorded automatically —
 * the waiters don't have to do anything extra.
 *
 * This is a Servlet Filter — it wraps around every HTTP call.
 * Flow: Request comes in → filter starts timer → your controller runs
 *       → filter captures response status + duration → publishes to SDK
 */
@Slf4j
@Component
@Order(1)   // Run this filter first, before any others
@RequiredArgsConstructor
public class HttpLoggingFilter implements Filter {

    private final LogAgentProperties properties;
    private final LogCaptureService logCaptureService;
    @Override
    public void doFilter(ServletRequest request,
                         ServletResponse response,
                         FilterChain chain) throws IOException, ServletException {

        HttpServletRequest  httpRequest  = (HttpServletRequest)  request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;

        String path = httpRequest.getRequestURI();

        // Skip excluded paths (health checks, actuator, etc.)
        if (isExcluded(path)) {
            chain.doFilter(request, response);
            return;
        }

        long startTime = System.currentTimeMillis();
        String traceId = getOrCreateTraceId(httpRequest);

        try {
            // Let the actual request proceed
            chain.doFilter(request, response);
        } finally {
            // This runs AFTER your controller finishes — even if it threw an exception
            long duration = System.currentTimeMillis() - startTime;
            int  status   = httpResponse.getStatus();

            // Determine log level based on HTTP status code
            LogEvent.LogLevel level = resolveLevel(status);

            // Skip if below minimum configured level
            if (level.ordinal() >= properties.getMinimumLevel().ordinal()) {
                String message = String.format("%s %s → %d (%dms)",
                        httpRequest.getMethod(), path, status, duration);

                logCaptureService.capture(
                        LogEvent.builder()
                                .eventId(UUID.randomUUID().toString())
                                .timestamp(startTime)
                                .level(level)
                                .serviceName(properties.getServiceName())
                                .environment(properties.getEnvironment())
                                .message(message)
                                .traceId(traceId)
                                .sourceIp(httpRequest.getRemoteAddr())
                                .category("HTTP_REQUEST")
                                .tags(java.util.Map.of(
                                        "method",   httpRequest.getMethod(),
                                        "path",     path,
                                        "status",   String.valueOf(status),
                                        "duration", duration + "ms"
                                ))
                                .build()
                );
            }
        }
    }


    private LogEvent.LogLevel resolveLevel(int httpStatus) {
        if (httpStatus >= 500) return LogEvent.LogLevel.ERROR;
        if (httpStatus >= 400) return LogEvent.LogLevel.WARN;
        return LogEvent.LogLevel.INFO;
    }

    private boolean isExcluded(String path) {
        return properties.getExcludedPaths().stream()
                .anyMatch(path::startsWith);
    }

    private String getOrCreateTraceId(HttpServletRequest request) {
        // If another service passed a trace ID in the header, use it
        // This is how distributed tracing works across multiple services
        String traceId = request.getHeader("X-Trace-Id");
        return (traceId != null && !traceId.isBlank())
                ? traceId
                : UUID.randomUUID().toString();
    }
}