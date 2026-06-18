package com.logforge.common.zookeeper;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Generic service registry backed by ZooKeeper ephemeral nodes.
 *
 * REAL LIFE ANALOGY:
 * A hotel check-in board. When a service instance starts, it "checks in"
 * by creating an ephemeral znode visible to everyone. If the service
 * crashes, ZooKeeper's session timeout removes the node automatically —
 * no manual cleanup, no stale entries.
 */
@Slf4j
public class ServiceRegistry {

    private final CuratorFramework client;
    private final String serviceName;
    private final String instanceId;

    public ServiceRegistry(CuratorFramework client, String serviceName) {
        this.client      = client;
        this.serviceName = serviceName;
        this.instanceId  = UUID.randomUUID().toString().substring(0, 8);
    }

    /** Register this instance — ephemeral node auto-removed if instance dies. */
    public void register(int port) {
        try {
            String host = InetAddress.getLocalHost().getHostName();
            String data = host + ":" + port;
            String path = ZkPaths.instancePath(serviceName, instanceId);

            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(path, data.getBytes(StandardCharsets.UTF_8));

            log.info("Registered service instance: {} → {}", path, data);
        } catch (Exception e) {
            log.error("Failed to register service in ZooKeeper: {}", e.getMessage());
        }
    }

    /** Discover all currently registered instances of a service by name. */
    public List<String> discover(String targetService) {
        try {
            String path = ZkPaths.servicePath(targetService);
            List<String> children = client.getChildren().forPath(path);
            return children.stream()
                    .map(child -> {
                        try {
                            byte[] data = client.getData().forPath(path + "/" + child);
                            return new String(data, StandardCharsets.UTF_8);
                        } catch (Exception e) {
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .toList();
        } catch (KeeperException.NoNodeException e) {
            return List.of(); // no instances registered yet
        } catch (Exception e) {
            log.error("Failed to discover service '{}': {}", targetService, e.getMessage());
            return List.of();
        }
    }

    public String getInstanceId() { return instanceId; }
}