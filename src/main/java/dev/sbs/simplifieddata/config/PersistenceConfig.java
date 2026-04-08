package dev.sbs.simplifieddata.config;

import dev.sbs.minecraftapi.MinecraftApi;
import dev.sbs.minecraftapi.persistence.SkyBlockFactory;
import dev.sbs.simplifieddata.client.SkyBlockDataContract;
import dev.sbs.simplifieddata.persistence.RemoteSkyBlockFactory;
import dev.sbs.simplifieddata.poller.LastResponseAccessor;
import dev.simplified.client.Client;
import dev.simplified.gson.GsonSettings;
import dev.simplified.persistence.CacheMissingStrategy;
import dev.simplified.persistence.JpaCacheProvider;
import dev.simplified.persistence.JpaConfig;
import dev.simplified.persistence.JpaSession;
import dev.simplified.persistence.RepositoryFactory;
import dev.simplified.persistence.SessionManager;
import dev.simplified.persistence.asset.ExternalAssetState;
import dev.simplified.persistence.driver.H2MemoryDriver;
import dev.simplified.persistence.source.FileFetcher;
import dev.simplified.persistence.source.IndexProvider;
import dev.simplified.util.Logging;
import jakarta.annotation.PostConstruct;
import lombok.extern.log4j.Log4j2;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;

/**
 * Wires the SkyBlock persistence layer for {@code simplified-data} against the dockerized
 * Hazelcast cluster defined in {@code infra/hazelcast/docker-compose.yml}, plus the
 * dedicated asset-state session and contract/accessor bridge beans used by the Phase 4c
 * {@link dev.sbs.simplifieddata.poller.AssetPoller}.
 *
 * <p>This class exposes two {@link JpaSession} beans:
 * <ul>
 *   <li>{@code skyBlockSession} - the SkyBlock entity session backed by
 *       {@link JpaCacheProvider#HAZELCAST_CLIENT}, scoped to
 *       {@code dev.sbs.minecraftapi.persistence.model}. Phase 5 wires its
 *       {@link RepositoryFactory} to a {@link RemoteSkyBlockFactory} so every repository
 *       loads its data from the {@code skyblock-data} GitHub repo via
 *       {@link dev.simplified.persistence.source.RemoteJsonSource} with an optional
 *       {@link dev.simplified.persistence.source.DiskOverlaySource} local override layer.</li>
 *   <li>{@link #assetSession()} - a second session scoped to the Phase 4a
 *       {@code dev.simplified.persistence.asset} package. Carries only
 *       {@link ExternalAssetState} and {@code ExternalAssetEntryState}. Uses
 *       {@link JpaCacheProvider#EHCACHE} rather than Hazelcast because the asset-state
 *       tables are single-writer in-process state with no cluster value, and avoiding the
 *       L2 handshake keeps the second session's startup path independent of Hazelcast.</li>
 * </ul>
 *
 * <p>The split is deliberate: {@code SkyBlockFactory} anchors its classpath scan at
 * {@code dev.sbs.minecraftapi.persistence.model.Item.class}, which does NOT prefix-match
 * the sibling {@code dev.simplified.persistence.asset} package, so the Phase 4a asset
 * entities are invisible to the SkyBlock session. Rather than coupling {@code minecraft-api}
 * to the asset schema (which is a Phase 5 concern), Phase 4c carries a dedicated second
 * session inside {@code simplified-data} and lets {@code AssetPoller} write to it via
 * {@link Qualifier @Qualifier("assetSession")}.
 *
 * <p>This class also exposes two bridge beans that decouple
 * {@link dev.sbs.simplifieddata.poller.AssetPoller} from the {@code final}
 * {@link Client} type: {@link #skyBlockDataContract(Client)} unwraps the Phase 4b client's
 * proxy, and {@link #skyBlockDataLastResponseAccessor(Client)} returns a method-reference
 * adapter so the poller's tests can supply hand-rolled stubs without subclassing the
 * framework.
 */
@Configuration
@Log4j2
public class PersistenceConfig {

    /**
     * Constructs the SkyBlock entity session backed by {@link JpaCacheProvider#HAZELCAST_CLIENT},
     * wiring a {@link RemoteSkyBlockFactory} so every repository loads its data from the
     * {@code skyblock-data} GitHub repo via {@link dev.simplified.persistence.source.RemoteJsonSource}
     * with an optional {@link dev.simplified.persistence.source.DiskOverlaySource} local override
     * layer.
     *
     * <p>This replaces the Phase 2c one-liner
     * {@code MinecraftApi.connectSkyBlockSession(JpaCacheProvider.HAZELCAST_CLIENT)}. Every
     * setting the one-liner preserved - driver, schema, Gson string-type mutation, query cache,
     * L2 cache, {@code READ_WRITE} concurrency, {@code CREATE_WARN} missing-cache strategy,
     * 30-second query TTL - is reproduced verbatim here because the {@link MinecraftApi}
     * overload hardcodes {@link SkyBlockFactory} and cannot accept a caller-supplied factory.
     * Drift between this block and the
     * {@code MinecraftApi.connectSkyBlockSession(JpaCacheProvider)} method is a Phase 5
     * regression hazard - if {@code minecraft-api} ever adds a new setting to the one-liner,
     * mirror it here.
     *
     * <p>Session ownership is local: the bean constructs a fresh {@link SessionManager} rather
     * than reusing {@link MinecraftApi#getSessionManager()}. This matches the
     * {@link #assetSession()} pattern and keeps shutdown cleanly scoped to the Spring context.
     *
     * @param gitHubIndexProvider the Phase 4b manifest index provider bean
     * @param gitHubFileFetcher the Phase 4b raw-file fetcher bean
     * @param overlayBasePath the base directory for {@link dev.simplified.persistence.source.DiskOverlaySource}
     *                        lookups, resolved from {@code skyblock.data.overlay.path}
     * @return the SkyBlock {@link JpaSession}
     */
    @Bean
    public @NotNull JpaSession skyBlockSession(
        @NotNull IndexProvider gitHubIndexProvider,
        @NotNull FileFetcher gitHubFileFetcher,
        @Value("${skyblock.data.overlay.path:skyblock-data-overlay}") @NotNull String overlayBasePath
    ) {
        SkyBlockFactory skyBlockFactory = MinecraftApi.getSkyBlockFactory();
        RepositoryFactory remoteFactory = new RemoteSkyBlockFactory(
            GitHubConfig.SOURCE_ID,
            skyBlockFactory,
            gitHubIndexProvider,
            gitHubFileFetcher,
            Path.of(overlayBasePath)
        );

        JpaConfig config = JpaConfig.builder()
            .withDriver(new H2MemoryDriver())
            .withSchema("skyblock")
            .withCacheProvider(JpaCacheProvider.HAZELCAST_CLIENT)
            .withRepositoryFactory(remoteFactory)
            .withGsonSettings(
                MinecraftApi.getServiceManager()
                    .get(GsonSettings.class)
                    .mutate()
                    .withStringType(GsonSettings.StringType.DEFAULT)
                    .build()
            )
            .withLogLevel(Logging.Level.WARN)
            .isUsingQueryCache()
            .isUsing2ndLevelCache()
            .withCacheConcurrencyStrategy(CacheConcurrencyStrategy.READ_WRITE)
            .withCacheMissingStrategy(CacheMissingStrategy.CREATE_WARN)
            .withQueryResultsTTL(30)
            .build();

        JpaSession session = new SessionManager().connect(config);
        log.info(
            "simplified-data skyBlock session wired with RemoteSkyBlockFactory (sourceId='{}', overlayBasePath='{}', cacheProvider=HAZELCAST_CLIENT)",
            GitHubConfig.SOURCE_ID, overlayBasePath
        );
        return session;
    }

    /**
     * Constructs the dedicated asset-state session used by the Phase 4c poller.
     *
     * <p>Uses a fresh {@link H2MemoryDriver} with schema {@code asset_state} and a
     * {@link RepositoryFactory} anchored at {@link ExternalAssetState} so the classpath
     * scan picks up both asset entities and nothing else. {@link JpaCacheProvider#EHCACHE}
     * is the provider because the asset-state tables are single-writer in-process state.
     *
     * <p>The bean delegates to a locally constructed {@link SessionManager} rather than
     * the shared {@link MinecraftApi#getSessionManager()}, so that shutdown of one session
     * does not cascade into the other. The returned {@code JpaSession} is registered with
     * the local manager and lives for the lifetime of the Spring context.
     *
     * @return the asset-state {@link JpaSession}
     */
    @Bean
    public @NotNull JpaSession assetSession() {
        JpaConfig config = JpaConfig.common(new H2MemoryDriver(), "asset_state")
            .withRepositoryFactory(
                RepositoryFactory.builder()
                    .withPackageOf(ExternalAssetState.class)
                    .build()
            )
            .withCacheProvider(JpaCacheProvider.EHCACHE)
            .build();

        JpaSession session = new SessionManager().connect(config);
        log.info("simplified-data asset-state session wired with JpaCacheProvider.EHCACHE (schema=asset_state)");
        return session;
    }

    /**
     * Unwraps the Phase 4b client's contract proxy as a stand-alone bean so that
     * {@link dev.sbs.simplifieddata.poller.AssetPoller} does not need to depend on the
     * {@code final} {@link Client} type directly. Tests substitute a hand-rolled
     * {@link SkyBlockDataContract} stub via the same constructor parameter.
     *
     * @param skyBlockDataClient the Phase 4b GitHub client wrapper
     * @return the unwrapped {@link SkyBlockDataContract} proxy
     */
    @Bean
    public @NotNull SkyBlockDataContract skyBlockDataContract(@NotNull Client<SkyBlockDataContract> skyBlockDataClient) {
        return skyBlockDataClient.getContract();
    }

    /**
     * Builds the {@link LastResponseAccessor} bridge bean as a method reference to
     * {@link Client#getLastResponse()}. This sidesteps the {@code final} class barrier so
     * tests can supply a synthetic last-response accessor without subclassing the
     * framework client.
     *
     * @param skyBlockDataClient the Phase 4b GitHub client wrapper
     * @return a method-reference accessor delegating to the wrapper's last-response cache
     */
    @Bean
    public @NotNull LastResponseAccessor skyBlockDataLastResponseAccessor(@NotNull Client<SkyBlockDataContract> skyBlockDataClient) {
        return skyBlockDataClient::getLastResponse;
    }

    @PostConstruct
    void logCacheProvider() {
        log.info("simplified-data persistence wired with JpaCacheProvider.HAZELCAST_CLIENT - "
            + "cluster member resolution via classpath:hazelcast-client.xml");
    }

}
