package com.logforge.stream.config;

import com.logforge.common.zookeeper.LeaderElectionService;
import com.logforge.common.zookeeper.ServiceRegistry;
import jakarta.annotation.PreDestroy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ZookeeperConfig {

    @Value("${logforge.zookeeper.connect-string:localhost:2181}")
    private String connectString;

    @Value("${server.port:8082}")
    private int servicePort;

    private CuratorFramework curatorFramework;

    @Bean
    public CuratorFramework curatorFramework() {
        curatorFramework = CuratorFrameworkFactory.newClient(
                connectString, new ExponentialBackoffRetry(1000, 3));
        curatorFramework.start();
        return curatorFramework;
    }

    @Bean
    public LeaderElectionService leaderElectionService(CuratorFramework client) {
        LeaderElectionService service =
                new LeaderElectionService(client, "stream-processor");
        service.start();
        return service;
    }

    @Bean
    public ServiceRegistry serviceRegistry(CuratorFramework client) {
        ServiceRegistry registry = new ServiceRegistry(client, "stream-processor");
        registry.register(servicePort);
        return registry;
    }

    @PreDestroy
    public void cleanup() {
        if (curatorFramework != null) curatorFramework.close();
    }
}