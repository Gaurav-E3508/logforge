package com.logforge.stream.controller;

import com.logforge.common.zookeeper.LeaderElectionService;
import com.logforge.common.zookeeper.ServiceRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Exposes cluster state — proves leader election and service discovery
 * are actually working.
 *
 * Run multiple instances of stream-processor on different ports and hit
 * this endpoint on each — exactly ONE should show isLeader: true.
 */
@RestController
@RequestMapping("/api/cluster")
@RequiredArgsConstructor
public class ClusterController {

    private final LeaderElectionService leaderElection;
    private final ServiceRegistry       serviceRegistry;

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        return ResponseEntity.ok(Map.of(
                "instanceId",          leaderElection.getInstanceId(),
                "isLeader",            leaderElection.isLeader(),
                "registeredInstances", serviceRegistry.discover("stream-processor")
        ));
    }
}