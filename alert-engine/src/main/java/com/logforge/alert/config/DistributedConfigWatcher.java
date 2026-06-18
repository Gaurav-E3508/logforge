package com.logforge.alert.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.logforge.alert.model.AlertRule;
import com.logforge.alert.service.AlertManager;
import com.logforge.common.zookeeper.ZkPaths;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.zookeeper.CreateMode;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Watches /logforge/config/alert-rules for changes and hot-reloads
 * AlertManager rules without restarting the service.
 *
 * REAL LIFE ANALOGY:
 * Instead of restarting every till machine to update pricing, you post
 * an updated price list on a shared notice board. Every till checks the
 * board and instantly applies the new prices — no restart needed.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DistributedConfigWatcher {

    private final CuratorFramework client;
    private final AlertManager     alertManager;
    private final ObjectMapper     objectMapper;

    private NodeCache nodeCache;

    @PostConstruct
    public void start() {
        try {
            String path = ZkPaths.ALERT_RULES_CONFIG;

            if (client.checkExists().forPath(path) == null) {
                client.create().creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(path, "[]".getBytes(StandardCharsets.UTF_8));
            }

            nodeCache = new NodeCache(client, path);
            nodeCache.getListenable().addListener(this::onConfigChanged);
            nodeCache.start(true);

            log.info("Distributed config watcher started on path: {}", path);
        } catch (Exception e) {
            log.error("Failed to start distributed config watcher: {}", e.getMessage());
        }
    }

    private void onConfigChanged() {
        try {
            byte[] data = nodeCache.getCurrentData().getData();
            String json = new String(data, StandardCharsets.UTF_8);

            if (json.isBlank() || json.equals("[]")) return;

            List<AlertRule> newRules = objectMapper.readValue(json,
                    objectMapper.getTypeFactory()
                            .constructCollectionType(List.class, AlertRule.class));

            newRules.forEach(alertManager::addRule);
            log.info("Hot-reloaded {} alert rule(s) from ZooKeeper config",
                    newRules.size());

        } catch (Exception e) {
            log.error("Failed to apply distributed config change: {}", e.getMessage());
        }
    }

    @PreDestroy
    public void stop() throws Exception {
        if (nodeCache != null) nodeCache.close();
    }
}