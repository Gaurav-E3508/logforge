package com.logforge.alert.config;

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

    private CuratorFramework curatorFramework;

    @Bean
    public CuratorFramework curatorFramework() {
        curatorFramework = CuratorFrameworkFactory.newClient(
                connectString, new ExponentialBackoffRetry(1000, 3));
        curatorFramework.start();
        return curatorFramework;
    }

    @PreDestroy
    public void cleanup() {
        if (curatorFramework != null) curatorFramework.close();
    }
}