package dev.sbs.simplifieddata;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Spring Boot application entry point for the SkyBlock Simplified data service.
 *
 * <p>Bootstraps a {@link dev.simplified.persistence.JpaSession} wired through
 * {@link dev.simplified.persistence.JpaCacheProvider#HAZELCAST_CLIENT} so that Hibernate
 * second-level cache regions live on the dockerized Hazelcast cluster defined in
 * {@code infra/hazelcast/docker-compose.yml}.</p>
 *
 * <p>Phase 2c scope: cluster connectivity proof + L2 cache visibility in Management Center.
 * Phase 4b added the GitHub client. Phase 4c adds the scheduled asset watchdog
 * ({@link dev.sbs.simplifieddata.poller.AssetPoller}) - {@link EnableScheduling} activates
 * Spring's {@code @Scheduled} runner so the poller's periodic method fires.</p>
 */
@SpringBootApplication
@EnableScheduling
public class SimplifiedData {

    public static void main(String[] args) {
        SpringApplication.run(SimplifiedData.class, args);
    }

}
