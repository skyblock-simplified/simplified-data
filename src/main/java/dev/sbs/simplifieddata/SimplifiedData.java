package dev.sbs.simplifieddata;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Spring Boot application entry point for the SkyBlock Simplified data service.
 *
 * <p>Bootstraps a {@link dev.simplified.persistence.JpaSession} wired through
 * {@link dev.simplified.persistence.JpaCacheProvider#HAZELCAST_CLIENT} so that Hibernate
 * second-level cache regions live on the dockerized Hazelcast cluster defined in
 * {@code infra/hazelcast/docker-compose.yml}.</p>
 *
 * <p>Phase 2c scope: cluster connectivity proof + L2 cache visibility in Management Center.
 * Controllers, IQueue consumers, and asset polling are added in later phases.</p>
 */
@SpringBootApplication
public class SimplifiedData {

    public static void main(String[] args) {
        SpringApplication.run(SimplifiedData.class, args);
    }

}
