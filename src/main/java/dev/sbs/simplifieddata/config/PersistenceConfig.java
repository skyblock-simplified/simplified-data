package dev.sbs.simplifieddata.config;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import dev.sbs.minecraftapi.MinecraftApi;
import dev.sbs.minecraftapi.persistence.SkyBlockFactory;
import dev.sbs.simplifieddata.client.SkyBlockDataContract;
import dev.sbs.simplifieddata.client.SkyBlockDataWriteContract;
import dev.sbs.simplifieddata.persistence.RemoteSkyBlockFactory;
import dev.sbs.simplifieddata.poller.LastResponseAccessor;
import dev.sbs.simplifieddata.poller.RefreshTrigger;
import dev.sbs.simplifieddata.write.WriteMetrics;
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
import jakarta.annotation.PreDestroy;
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
     * <p>Phase 6b split the factory construction into a dedicated
     * {@link #remoteSkyBlockFactory} bean so the
     * {@link dev.sbs.simplifieddata.write.WriteQueueConsumer} and
     * {@link dev.sbs.simplifieddata.write.WriteBatchScheduler} can inject the
     * factory directly and iterate its
     * {@link RemoteSkyBlockFactory#getWritableSources() writable source registry}
     * without reaching back through the session.
     *
     * @param remoteSkyBlockFactory the Phase 6b factory bean wiring writable sources
     * @param overlayBasePath the base directory for {@link dev.simplified.persistence.source.DiskOverlaySource}
     *                        lookups (logged for observability; actual path already
     *                        consumed by {@link #remoteSkyBlockFactory})
     * @return the SkyBlock {@link JpaSession}
     */
    @Bean
    public @NotNull RemoteSkyBlockFactory remoteSkyBlockFactory(
        @NotNull IndexProvider gitHubIndexProvider,
        @NotNull FileFetcher gitHubFileFetcher,
        @NotNull SkyBlockDataWriteContract skyBlockDataWriteContract,
        @NotNull WriteMetrics writeMetrics,
        @Value("${skyblock.data.overlay.path:skyblock-data-overlay}") @NotNull String overlayBasePath,
        @Value("${skyblock.data.github.write-412-immediate-retries:3}") int max412ImmediateRetries
    ) {
        SkyBlockFactory skyBlockFactory = MinecraftApi.getSkyBlockFactory();
        return new RemoteSkyBlockFactory(
            GitHubConfig.SOURCE_ID,
            skyBlockFactory,
            gitHubIndexProvider,
            gitHubFileFetcher,
            skyBlockDataWriteContract,
            MinecraftApi.getGson(),
            Path.of(overlayBasePath),
            max412ImmediateRetries,
            writeMetrics
        );
    }

    @Bean
    public @NotNull JpaSession skyBlockSession(
        @NotNull RemoteSkyBlockFactory remoteSkyBlockFactory,
        @Value("${skyblock.data.overlay.path:skyblock-data-overlay}") @NotNull String overlayBasePath
    ) {
        JpaConfig config = JpaConfig.builder()
            .withDriver(new H2MemoryDriver())
            .withSchema("skyblock")
            .withCacheProvider(JpaCacheProvider.HAZELCAST_CLIENT)
            .withRepositoryFactory(remoteSkyBlockFactory)
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

    /**
     * Builds the Phase 5.5 {@link RefreshTrigger} bridge bean as a method reference to
     * {@link JpaSession#refreshModels(java.util.Collection)} on the SkyBlock session.
     *
     * <p>The poller fires this bridge after a successful {@code AssetPoller.applyDiff()}
     * commit to propagate GitHub-detected changes into the in-memory entity caches.
     * Wrapping the call in a SAM lets the poller stay decoupled from the {@code final}
     * {@link JpaSession} type so tests can inject a plain lambda.
     *
     * @param skyBlockSession the SkyBlock session (Phase 5 Hazelcast-backed session)
     * @return a method-reference refresh trigger delegating to the SkyBlock session
     */
    @Bean
    public @NotNull RefreshTrigger skyBlockRefreshTrigger(@NotNull @Qualifier("skyBlockSession") JpaSession skyBlockSession) {
        return skyBlockSession::refreshModels;
    }

    /**
     * Field carrying the Phase 6b write-path Hazelcast client so that
     * {@link #shutdownWriteHazelcastInstance()} can close it on Spring
     * context teardown. The field is populated by
     * {@link #skyBlockWriteHazelcastInstance()}.
     */
    private volatile HazelcastInstance writeHazelcastInstance;

    /**
     * Constructs the Phase 6b write-path {@link HazelcastInstance} as a
     * second Hazelcast client on top of the classpath
     * {@code hazelcast-client.xml} configuration, alongside the JCache-managed
     * instance inside the {@code skyBlockSession}'s Hibernate L2 region.
     *
     * <p>Option A from the Phase 6b Q1 locked decision - two client instances
     * against the same cluster membership (cluster name {@code skyblock}).
     * Slightly wasteful in connection count but the cleanest way to get a raw
     * {@link HazelcastInstance} bean for direct IQueue / IMap access without
     * coupling to the JCache provider's internal
     * {@code CacheManager.unwrap(HazelcastInstance.class)} path (which would
     * hard-bind the write path to the Hibernate L2 implementation detail).
     *
     * <p>Hazelcast supports multiple client instances per JVM natively -
     * {@link HazelcastClient#newHazelcastClient()} uses
     * {@code ClientConfig.load()} to discover the classpath XML the same way
     * the JCache path does, and each call returns a fresh unique instance.
     * Shutdown is managed via {@link #shutdownWriteHazelcastInstance()} so
     * the connection pool is released cleanly on Spring context teardown.
     *
     * <p>The bean is named {@code skyBlockWriteHazelcastInstance} (not the
     * bare {@code hazelcastInstance}) to make the purpose explicit and
     * prevent accidental injection into unrelated beans.
     *
     * @return a fresh Hazelcast client instance connected to the
     *         {@code skyblock} cluster
     */
    @Bean
    public @NotNull HazelcastInstance skyBlockWriteHazelcastInstance() {
        HazelcastInstance instance = HazelcastClient.newHazelcastClient();
        this.writeHazelcastInstance = instance;
        log.info(
            "simplified-data write path: Hazelcast client '{}' connected to cluster '{}'",
            instance.getName(), instance.getConfig().getClusterName()
        );
        return instance;
    }

    @PreDestroy
    void shutdownWriteHazelcastInstance() {
        HazelcastInstance instance = this.writeHazelcastInstance;

        if (instance != null) {
            try {
                instance.shutdown();
                log.info("simplified-data write path: Hazelcast client '{}' shut down cleanly", instance.getName());
            } catch (Throwable ex) {
                log.warn("simplified-data write path: Hazelcast client shutdown raised exception (ignored)", ex);
            }
        }
    }

    @PostConstruct
    void logCacheProvider() {
        log.info("simplified-data persistence wired with JpaCacheProvider.HAZELCAST_CLIENT - "
            + "cluster member resolution via classpath:hazelcast-client.xml");
    }

}
