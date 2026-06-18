package com.logforge.common.zookeeper;

/**
 * Central registry of ZooKeeper paths used across LogForge.
 * Mirrors the pattern of KafkaTopics — one source of truth for znode paths.
 */
public final class ZkPaths {

    private ZkPaths() {}

    public static final String ROOT            = "/logforge";
    public static final String SERVICES        = ROOT + "/services";
    public static final String LEADER_ELECTION = ROOT + "/leader-election";
    public static final String CONFIG          = ROOT + "/config";
    public static final String ALERT_RULES_CONFIG = CONFIG + "/alert-rules";

    public static String leaderPath(String electionGroup) {
        return LEADER_ELECTION + "/" + electionGroup;
    }

    public static String servicePath(String serviceName) {
        return SERVICES + "/" + serviceName;
    }

    public static String instancePath(String serviceName, String instanceId) {
        return servicePath(serviceName) + "/" + instanceId;
    }
}