package com.logforge.common.zookeeper;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;

import java.io.IOException;
import java.util.UUID;

/**
 * Wraps Curator's LeaderLatch for simple leader election.
 *
 * HOW IT WORKS UNDER THE HOOD:
 * Every instance tries to create a sequential ephemeral znode under
 * the same parent path. ZooKeeper guarantees only the instance holding
 * the LOWEST sequence number is "leader". If that instance crashes,
 * its ephemeral znode disappears (session timeout), and the next-lowest
 * sequence number instance automatically becomes leader — no polling,
 * no manual failover logic needed.
 *
 * USAGE:
 *   if (leaderElection.isLeader()) {
 *       // only the leader does this work
 *   }
 */
@Slf4j
public class LeaderElectionService {

    private final CuratorFramework client;
    private final String latchPath;
    private final String instanceId;
    @Getter
    private LeaderLatch leaderLatch;

    public LeaderElectionService(CuratorFramework client, String electionGroup) {
        this.client     = client;
        this.latchPath  = ZkPaths.leaderPath(electionGroup);
        this.instanceId = "instance-" + UUID.randomUUID().toString().substring(0, 8);
    }

    public void start() {
        try {
            leaderLatch = new LeaderLatch(client, latchPath, instanceId);
            leaderLatch.start();
            log.info("Leader election started — path: {}, instanceId: {}",
                    latchPath, instanceId);
        } catch (Exception e) {
            log.error("Failed to start leader election: {}", e.getMessage());
        }
    }

    public boolean isLeader() {
        return leaderLatch != null && leaderLatch.hasLeadership();
    }

    public void stop() {
        try {
            if (leaderLatch != null) leaderLatch.close();
        } catch (IOException e) {
            log.warn("Error closing leader latch: {}", e.getMessage());
        }
    }

    public String getInstanceId() { return instanceId; }
}