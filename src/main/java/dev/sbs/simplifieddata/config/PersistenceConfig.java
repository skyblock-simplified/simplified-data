package dev.sbs.simplifieddata.config;

import dev.sbs.minecraftapi.MinecraftApi;
import dev.simplified.persistence.JpaCacheProvider;
import dev.simplified.persistence.JpaSession;
import jakarta.annotation.PostConstruct;
import lombok.extern.log4j.Log4j2;
import org.jetbrains.annotations.NotNull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the SkyBlock persistence layer for {@code simplified-data} against the dockerized
 * Hazelcast cluster defined in {@code infra/hazelcast/docker-compose.yml}.
 *
 * <p>This bean is the explicit-connect site for {@code simplified-data}. As of {@code minecraft-api}
 * commit {@code 1e24c10}, the {@link MinecraftApi} static initializer no longer auto-connects the
 * SkyBlock {@link JpaSession}; every consumer must call {@link MinecraftApi#connectSkyBlockSession()}
 * (or its provider overload) at a deterministic point in its own startup sequence. For
 * {@code simplified-data} that point is Spring context refresh, expressed as the
 * {@link #skyBlockSession()} bean below.</p>
 *
 * <p>The bean delegates to {@link MinecraftApi#connectSkyBlockSession(JpaCacheProvider)} with
 * {@link JpaCacheProvider#HAZELCAST_CLIENT}, which preserves every locked-correct
 * {@code MinecraftApi.connectSkyBlockSession()} setting (H2 in-memory driver, schema
 * {@code "skyblock"}, {@link MinecraftApi#getSkyBlockFactory()} as the {@code RepositoryFactory},
 * {@code GsonSettings.StringType.DEFAULT} mutation, query cache, second-level cache,
 * {@code READ_WRITE} concurrency, {@code CREATE_WARN} missing-cache strategy, 30-second query TTL)
 * and varies only the cache provider. Hibernate's {@code JCacheRegionFactory} then loads
 * {@code hazelcast-client.xml} from the classpath and connects this JVM as a Hazelcast Java client
 * to the docker container named {@code hazelcast} on {@code skyblock-hazelcast-net}.</p>
 */
@Configuration
@Log4j2
public class PersistenceConfig {

    @Bean
    public @NotNull JpaSession skyBlockSession() {
        return MinecraftApi.connectSkyBlockSession(JpaCacheProvider.HAZELCAST_CLIENT);
    }

    @PostConstruct
    void logCacheProvider() {
        log.info("simplified-data persistence wired with JpaCacheProvider.HAZELCAST_CLIENT - "
            + "cluster member resolution via classpath:hazelcast-client.xml");
    }

}
