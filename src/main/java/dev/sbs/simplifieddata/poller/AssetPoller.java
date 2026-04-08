package dev.sbs.simplifieddata.poller;

import com.google.gson.Gson;
import dev.sbs.minecraftapi.MinecraftApi;
import dev.sbs.simplifieddata.client.ETagContext;
import dev.sbs.simplifieddata.client.SkyBlockDataContract;
import dev.sbs.simplifieddata.client.exception.SkyBlockDataException;
import dev.sbs.simplifieddata.client.response.GitHubCommit;
import dev.sbs.simplifieddata.config.GitHubConfig;
import dev.simplified.client.response.Response;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.JpaSession;
import dev.simplified.persistence.asset.ExternalAssetEntryState;
import dev.simplified.persistence.asset.ExternalAssetState;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.source.ManifestIndex;
import lombok.extern.log4j.Log4j2;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Optional;

/**
 * Scheduled watchdog that detects changes in the {@code skyblock-data} GitHub repository
 * and maintains the Phase 4a {@link ExternalAssetState} / {@link ExternalAssetEntryState}
 * audit tables.
 *
 * <p>Phase 4c scope is deliberately watchdog-only: the poller updates state tables and logs
 * detected changes but does NOT mutate any SkyBlock entity repository. Phase 5 wires
 * {@code RemoteJsonSource} into the SkyBlock factory and makes the "change detected" signal
 * actionable via targeted repository refreshes.
 *
 * <p>Poll cadence is driven by a Spring {@code @Scheduled(fixedDelayString=...)} annotation
 * that reads {@code skyblock.data.github.poll-interval-seconds} (default {@code 60}). The
 * {@code fixedDelay} variant (not {@code fixedRate}) guarantees that a slow cycle never
 * overlaps the next one - the next fire waits until the previous invocation returns.
 *
 * <p>Startup behaviour: an {@code @EventListener(ApplicationReadyEvent.class)} method
 * triggers the first poll immediately after Spring finishes refreshing the context, so the
 * asset-state tables are populated on first boot without waiting for the scheduled delay.
 * Both entry points forward to the private {@link #doPoll(String)} method so there is one
 * implementation of the cycle.
 *
 * <p>Graceful degradation is load-bearing. A GitHub error ({@link SkyBlockDataException})
 * is logged at {@code WARN} with the rate-limit disambiguation helpers; a persistence
 * failure ({@link JpaException}) is logged at {@code ERROR}. Either case leaves the service
 * running so the next cycle can retry. The poller never throws out of {@link #doPoll(String)}.
 *
 * <p>Transaction boundary: HTTP calls run OUTSIDE the {@link JpaSession#transaction} block,
 * so the database connection is never held open across network I/O. Only the writes
 * (upserts, deletes, state update) execute inside a single transaction that either commits
 * atomically or rolls back, leaving the previous cycle's state intact.
 *
 * <p>Test indirection: the poller takes a {@link LastResponseAccessor} bridge instead of
 * the raw {@link dev.simplified.client.Client} wrapper because the production
 * {@code Client<C>} class is {@code final} with a {@code private} constructor and cannot
 * be subclassed in tests. Production wires the bridge as {@code client::getLastResponse};
 * tests supply a hand-rolled lambda returning a synthetic {@link Response}.
 *
 * @see GitHubConfig
 * @see AssetDiffEngine
 * @see ExternalAssetState
 * @see LastResponseAccessor
 */
@Component
@Log4j2
public class AssetPoller {

    /** Manifest path inside the {@code skyblock-data} repo (mirrors Phase 4b's constant). */
    private static final @NotNull String MANIFEST_PATH = "data/v1/index.json";

    /** Lowercase header key for the ETag response header. Framework normalizes casing through this name. */
    private static final @NotNull String ETAG_HEADER = "etag";

    /** The asset-state session (NOT the SkyBlock session). Scoped to {@code dev.simplified.persistence.asset}. */
    private final @NotNull JpaSession assetSession;

    /** The Phase 4b GitHub contract proxy. */
    private final @NotNull SkyBlockDataContract contract;

    /** The decoupled accessor for {@code Client.getLastResponse()}. */
    private final @NotNull LastResponseAccessor lastResponseAccessor;

    /** The Gson instance used to parse the manifest body. Reuses {@code MinecraftApi.getGson()} for {@code ConcurrentList} support. */
    private final @NotNull Gson gson;

    /** The {@code ExternalAssetState.sourceId} natural key used by every poll cycle. */
    private final @NotNull String sourceId;

    /** Whether scheduled cycles actually fire. Tests pass {@code false} to bypass network I/O. */
    private final boolean pollEnabled;

    /**
     * Constructs the poller with the injected asset-state session, GitHub contract, last-response
     * accessor, and configuration properties.
     *
     * @param assetSession the dedicated asset-state JPA session (NOT the SkyBlock session)
     * @param contract the Phase 4b GitHub contract proxy
     * @param lastResponseAccessor bridge to the underlying client's last-response cache
     * @param sourceId the {@link ExternalAssetState#getSourceId()} natural key
     * @param pollEnabled whether the scheduled and startup entry points should actually run
     */
    public AssetPoller(
        @Qualifier("assetSession") @NotNull JpaSession assetSession,
        @NotNull SkyBlockDataContract contract,
        @NotNull LastResponseAccessor lastResponseAccessor,
        @Value("${skyblock.data.github.source-id:skyblock-data}") @NotNull String sourceId,
        @Value("${skyblock.data.github.poll-enabled:true}") boolean pollEnabled
    ) {
        this.assetSession = assetSession;
        this.contract = contract;
        this.lastResponseAccessor = lastResponseAccessor;
        this.gson = MinecraftApi.getGson();
        this.sourceId = sourceId;
        this.pollEnabled = pollEnabled;
    }

    /**
     * Runs the startup poll cycle once the Spring context is fully initialised.
     *
     * <p>Fires after every {@code @Bean} is constructed, including {@code assetSession} and
     * the Phase 4b {@code skyBlockDataClient}, so both the database and the HTTP client are
     * guaranteed ready. Cannot race with {@link #scheduledPoll()} because the scheduled
     * runner's first fire is delayed by {@code fixedDelay} milliseconds after the bean
     * becomes schedulable.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        if (!this.pollEnabled) {
            log.debug("AssetPoller disabled (skyblock.data.github.poll-enabled=false) - skipping startup poll");
            return;
        }

        log.info("AssetPoller startup poll begin - sourceId='{}' cause=ApplicationReadyEvent", this.sourceId);
        this.doPoll("startup");
    }

    /**
     * Runs the scheduled poll cycle on the configured interval.
     *
     * <p>Uses {@code fixedDelayString} so a slow cycle never overlaps the next fire; Spring
     * waits for this method to return before scheduling the next invocation.
     */
    @Scheduled(fixedDelayString = "${skyblock.data.github.poll-interval-seconds:60}000")
    public void scheduledPoll() {
        if (!this.pollEnabled) {
            log.debug("AssetPoller disabled (skyblock.data.github.poll-enabled=false) - skipping scheduled cycle");
            return;
        }

        log.debug("AssetPoller scheduled poll begin - sourceId='{}'", this.sourceId);
        this.doPoll("scheduled");
    }

    /**
     * Executes one full poll cycle. Never throws - every failure path logs and returns so
     * the next scheduled fire can retry cleanly.
     *
     * @param cause a short tag identifying the trigger ({@code "startup"} or {@code "scheduled"})
     *              purely for log correlation
     */
    private void doPoll(@NotNull String cause) {
        try {
            ExternalAssetState currentState = this.loadCurrentState();
            String storedEtag = currentState.getEtag().orElse(null);
            String storedCommitSha = currentState.getCommitSha().orElse(null);

            Optional<CommitProbe> probe = this.probeLatestCommit(storedEtag);

            if (probe.isEmpty()) {
                this.recordNoChange();
                log.debug("AssetPoller source='{}' - no change (304 Not Modified, etag='{}')", this.sourceId, storedEtag);
                return;
            }

            CommitProbe fresh = probe.get();

            if (fresh.commitSha().equals(storedCommitSha)) {
                this.recordNoChange();
                log.debug(
                    "AssetPoller source='{}' - commit sha unchanged (etag='{}' -> '{}')",
                    this.sourceId, storedEtag, fresh.etag()
                );
                return;
            }

            log.info(
                "AssetPoller source='{}' - commit change detected: oldSha='{}' newSha='{}' (cause={})",
                this.sourceId, storedCommitSha, fresh.commitSha(), cause
            );

            ManifestSnapshot manifestSnapshot = this.fetchManifestSnapshot();
            ConcurrentList<ExternalAssetEntryState> persistedEntries = this.loadPersistedEntries();
            AssetDiff diff = AssetDiffEngine.compute(manifestSnapshot.manifest(), persistedEntries);

            log.info(
                "AssetPoller source='{}' - diff: added={} changed={} removed={}",
                this.sourceId, diff.getAdded().size(), diff.getChanged().size(), diff.getRemoved().size()
            );

            this.applyDiff(fresh, manifestSnapshot, diff);
        } catch (SkyBlockDataException ex) {
            log.warn(
                "AssetPoller source='{}' - GitHub call failed (HTTP {}): {} (primaryRateLimit={}, secondaryRateLimit={}, permissions={})",
                this.sourceId,
                ex.getStatus().getCode(),
                ex.getGithubResponse().getReason(),
                ex.isPrimaryRateLimit(),
                ex.isSecondaryRateLimit(),
                ex.isPermissions()
            );
        } catch (JpaException ex) {
            log.error("AssetPoller source='{}' - state write failed: {}", this.sourceId, ex.getMessage(), ex);
        } catch (RuntimeException ex) {
            log.error("AssetPoller source='{}' - unexpected poll failure: {}", this.sourceId, ex.getMessage(), ex);
        }
    }

    /**
     * Loads the current {@link ExternalAssetState} row for {@link #sourceId}, returning a
     * freshly-constructed default row when none exists yet (first boot).
     *
     * @return the persisted state row, or a fresh default when absent
     */
    private @NotNull ExternalAssetState loadCurrentState() {
        return this.assetSession.with(session -> {
            ExternalAssetState loaded = session.find(ExternalAssetState.class, this.sourceId);

            if (loaded != null)
                return loaded;

            ExternalAssetState fresh = new ExternalAssetState();
            fresh.setSourceId(this.sourceId);
            fresh.setLastCheckedAt(Instant.EPOCH);
            return fresh;
        });
    }

    /**
     * Issues the conditional Commits API call and returns the parsed commit sha + ETag header,
     * or {@link Optional#empty()} on a 304 short-circuit.
     *
     * <p>The conditional request is attached via {@link ETagContext#callWithEtag} when a stored
     * ETag is available; otherwise the call is unconditional and every response counts against
     * the rate-limit budget.
     *
     * @param storedEtag the previously-observed ETag, or {@code null} for an unconditional call
     * @return a {@link CommitProbe} on 200, or empty on 304
     */
    private @NotNull Optional<CommitProbe> probeLatestCommit(@Nullable String storedEtag) {
        if (storedEtag == null) {
            this.contract.getLatestMasterCommit();
        } else {
            ETagContext.callWithEtag(storedEtag, this.contract::getLatestMasterCommit);
        }

        Optional<Response<?>> lastResponse = this.lastResponseAccessor.getLastResponse();

        if (lastResponse.isEmpty())
            throw new IllegalStateException("Framework did not cache a response for getLatestMasterCommit");

        Response<?> response = lastResponse.get();
        int statusCode = response.getStatus().getCode();

        if (statusCode == 304)
            return Optional.empty();

        String etag = response.getHeaders()
            .getOptional(ETAG_HEADER)
            .flatMap(values -> values.stream().findFirst())
            .orElseThrow(() -> new IllegalStateException("Commits API 200 response missing ETag header"));

        Object body = response.getBody();

        if (!(body instanceof ConcurrentList<?> list) || list.isEmpty())
            throw new IllegalStateException("Commits API 200 response body is not a non-empty commit list");

        Object firstEntry = list.get(0);

        if (!(firstEntry instanceof GitHubCommit commit))
            throw new IllegalStateException("Commits API 200 first entry is not a GitHubCommit: " + firstEntry.getClass());

        return Optional.of(new CommitProbe(commit.getSha(), etag));
    }

    /**
     * Fetches the manifest raw JSON body and returns both the parsed {@link ManifestIndex}
     * and the content SHA-256 of the raw bytes.
     *
     * <p>Bypasses {@code GitHubIndexProvider} so the poller has direct access to the raw
     * bytes for hashing. {@code GitHubIndexProvider} remains the Phase 5 consumer path and
     * is unchanged.
     *
     * @return the snapshot carrying both views of the manifest
     */
    private @NotNull ManifestSnapshot fetchManifestSnapshot() {
        String rawJson = this.contract.getFileContent(MANIFEST_PATH);
        String contentSha = sha256Hex(rawJson.getBytes(StandardCharsets.UTF_8));
        ManifestIndex manifest = this.gson.fromJson(rawJson, ManifestIndex.class);

        if (manifest == null)
            throw new IllegalStateException("GitHub returned empty manifest body for source '" + this.sourceId + "'");

        return new ManifestSnapshot(manifest, contentSha);
    }

    /**
     * Loads all persisted entry-state rows for {@link #sourceId}.
     *
     * @return the current entry-state rows, possibly empty
     */
    private @NotNull ConcurrentList<ExternalAssetEntryState> loadPersistedEntries() {
        return this.assetSession.with(session -> {
            ConcurrentList<ExternalAssetEntryState> entries = Concurrent.newList();
            session.createQuery(
                "SELECT e FROM ExternalAssetEntryState e WHERE e.sourceId = :sid",
                ExternalAssetEntryState.class
            ).setParameter("sid", this.sourceId).getResultList().forEach(entries::add);
            return entries;
        });
    }

    /**
     * Persists a no-change cycle by bumping {@code lastCheckedAt} (and {@code lastSuccessAt})
     * on the existing state row inside a transaction.
     */
    private void recordNoChange() {
        Instant now = Instant.now();
        this.assetSession.transaction(session -> {
            ExternalAssetState managed = session.find(ExternalAssetState.class, this.sourceId);

            if (managed == null) {
                ExternalAssetState fresh = new ExternalAssetState();
                fresh.setSourceId(this.sourceId);
                fresh.setLastCheckedAt(now);
                fresh.setLastSuccessAt(now);
                session.persist(fresh);
            } else {
                managed.setLastCheckedAt(now);
                managed.setLastSuccessAt(now);
            }
        });
        log.debug("AssetPoller source='{}' - lastCheckedAt bumped (no commit change)", this.sourceId);
    }

    /**
     * Applies the computed diff to the asset-state tables inside a single transaction.
     *
     * <p>Order of operations inside the transaction:
     * <ol>
     *   <li>Upsert the {@link ExternalAssetState} row with fresh commit sha, ETag, content hash, and timestamps.</li>
     *   <li>Delete every {@link ExternalAssetEntryState} row in {@link AssetDiff#getRemoved()}.</li>
     *   <li>Persist every new entry in {@link AssetDiff#getAdded()}.</li>
     *   <li>Update every changed entry in {@link AssetDiff#getChanged()} to the new {@code content_sha256}.</li>
     * </ol>
     *
     * @param probe the fresh ETag + commit sha from the Commits API
     * @param snapshot the fetched manifest with its content SHA-256
     * @param diff the per-entry delta computed by {@link AssetDiffEngine}
     */
    private void applyDiff(
        @NotNull CommitProbe probe,
        @NotNull ManifestSnapshot snapshot,
        @NotNull AssetDiff diff
    ) {
        Instant now = Instant.now();
        this.assetSession.transaction(session -> {
            ExternalAssetState managed = session.find(ExternalAssetState.class, this.sourceId);

            if (managed == null) {
                managed = new ExternalAssetState();
                managed.setSourceId(this.sourceId);
                session.persist(managed);
            }

            managed.setEtag(probe.etag());
            managed.setCommitSha(probe.commitSha());
            managed.setContentSha256(snapshot.contentSha256());
            managed.setLastCheckedAt(now);
            managed.setLastSuccessAt(now);

            for (ExternalAssetEntryState staleRow : diff.getRemoved()) {
                ExternalAssetEntryState.PK pk = new ExternalAssetEntryState.PK(this.sourceId, staleRow.getEntryPath());
                ExternalAssetEntryState managedStale = session.find(ExternalAssetEntryState.class, pk);

                if (managedStale != null)
                    session.remove(managedStale);
            }

            for (ManifestIndex.Entry entry : diff.getAdded()) {
                ExternalAssetEntryState row = new ExternalAssetEntryState();
                row.setSourceId(this.sourceId);
                row.setEntryPath(entry.getPath());
                row.setEntrySha256(entry.getContentSha256());
                row.setLastSeenAt(now);
                session.persist(row);
            }

            for (AssetDiff.ChangedEntry changed : diff.getChanged()) {
                ExternalAssetEntryState.PK pk = new ExternalAssetEntryState.PK(this.sourceId, changed.getCurrent().getPath());
                ExternalAssetEntryState row = session.find(ExternalAssetEntryState.class, pk);

                if (row == null) {
                    row = new ExternalAssetEntryState();
                    row.setSourceId(this.sourceId);
                    row.setEntryPath(changed.getCurrent().getPath());
                    session.persist(row);
                }

                row.setEntrySha256(changed.getCurrent().getContentSha256());
                row.setLastSeenAt(now);
            }
        });

        log.info(
            "AssetPoller source='{}' - state written: commitSha='{}' contentSha256='{}' lastSuccessAt={}",
            this.sourceId, probe.commitSha(), snapshot.contentSha256(), now
        );
    }

    /**
     * Returns the lowercase hex SHA-256 digest of the given bytes.
     *
     * @param bytes the input bytes
     * @return lowercase hex digest
     */
    private static @NotNull String sha256Hex(@NotNull byte[] bytes) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(bytes));
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 not available on this JVM", ex);
        }
    }

    /**
     * Immutable probe result carrying the fresh commit sha and ETag header value from a
     * successful Commits API call.
     */
    private record CommitProbe(@NotNull String commitSha, @NotNull String etag) { }

    /**
     * Immutable manifest fetch result carrying the parsed {@link ManifestIndex} and the
     * SHA-256 of the raw bytes the manifest was parsed from.
     */
    private record ManifestSnapshot(@NotNull ManifestIndex manifest, @NotNull String contentSha256) { }

}
