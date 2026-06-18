package com.logforge.alert.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.logforge.alert.model.AlertRule;
import com.logforge.common.zookeeper.ZkPaths;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Publishes new alert rules to the shared ZooKeeper config node.
 * Any running alert-engine instance (including this one) will pick up
 * the change via DistributedConfigWatcher within milliseconds.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ConfigPublisher {

    private final CuratorFramework client;
    private final ObjectMapper     objectMapper;

    public void publishRules(List<AlertRule> rules) {
        try {
            String json = objectMapper.writeValueAsString(rules);
            String path = ZkPaths.ALERT_RULES_CONFIG;

            if (client.checkExists().forPath(path) == null) {
                client.create().creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(path, json.getBytes(StandardCharsets.UTF_8));
            } else {
                client.setData().forPath(path, json.getBytes(StandardCharsets.UTF_8));
            }

            log.info("Published {} rule(s) to distributed config", rules.size());
        } catch (Exception e) {
            log.error("Failed to publish distributed config: {}", e.getMessage());
        }
    }
}