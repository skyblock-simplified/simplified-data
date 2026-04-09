package dev.sbs.simplifieddata.poller;

import com.google.gson.Gson;
import dev.sbs.minecraftapi.MinecraftApi;
import dev.sbs.simplifieddata.client.SkyBlockDataContract;
import dev.sbs.simplifieddata.client.exception.SkyBlockDataException;
import dev.sbs.simplifieddata.client.response.GitHubCommit;
import dev.sbs.simplifieddata.config.GitHubConfig;
import dev.simplified.client.exception.NotModifiedException;
import dev.simplified.client.response.Response;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.JpaSession;
import dev.simplified.persistence.asset.ExternalAssetEntryState;
import dev.simplified.persistence.asset.ExternalAssetState;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.source.ManifestIndex;
import lombok.extern.log4j.Log4j2;
import org.jetbrains.annotations.NotNull;
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
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Scheduled watchdog that detects changes in the {@code skyblock-data} GitHub repository
 * and maintains the Phase 4a {@link ExternalAssetState} / {@link ExternalAssetEntryState}
 * audit tables, then fires a targeted refresh against the SkyBlock session for the
 * models whose underlying files actually changed.
 *
 * <p>Phase 4c introduced the watchdog write path. Phase 5 wired
 * {@link dev.simplified.persistence.source.RemoteJsonSource} into the SkyBlock factory
 * so the session's repositories fetch from the remote manifest. Phase 5.5 closes the
 * loop: after a successful {@code applyDiff()} commit, the poller resolves the set of
 * affected model classes from the diff's {@code added + changed} entries (each carrying
 * an FQCN in {@link ManifestIndex.Entry#getModelClass()}), then invokes the
 * {@link RefreshTrigger} bridge, which delegates to
 * {@link JpaSession#refreshModels(java.util.Collection)}. Data changes on GitHub now
 * propagate within one poll cycle ({@code ~60s}) instead of waiting for the next
 * container restart.
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

    /**
     * Bridge to {@link JpaSession#refreshModels(java.util.Collection)} on the SkyBlock
     * session, fired after a successful {@link #applyDiff} commit to propagate GitHub
     * changes into the in-memory entity caches. Wrapped in a SAM so tests can supply a
     * plain lambda without needing a live {@link JpaSession}.
     */
    private final @NotNull RefreshTrigger refreshTrigger;

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
     * Guards concurrent {@link #doPoll(String)} invocations so the startup poll triggered by
     * {@link ApplicationReadyEvent} on the main thread cannot race the first
     * {@link Scheduled} fire on the scheduling pool. On first boot both paths fire at roughly
     * the same instant, both read an empty {@link ExternalAssetState}, both attempt to INSERT
     * a new row keyed on {@code sourceId}, and one of them trips the primary-key constraint.
     * {@link ReentrantLock#tryLock()} lets the second caller skip its cycle cleanly with a
     * single {@code DEBUG} log line instead of emitting a confusing {@code ERROR}.
     */
    private final @NotNull ReentrantLock pollLock = new ReentrantLock();

    /**
     * Constructs the poller with the injected asset-state session, SkyBlock refresh trigger,
     * GitHub contract, last-response accessor, and configuration properties.
     *
     * @param assetSession the dedicated asset-state JPA session (NOT the SkyBlock session)
     * @param refreshTrigger Phase 5.5 bridge to the SkyBlock session's
     *                       {@code refreshModels} method, fired after a successful diff apply
     * @param contract the Phase 4b GitHub contract proxy
     * @param lastResponseAccessor bridge to the underlying client's last-response cache
     * @param sourceId the {@link ExternalAssetState#getSourceId()} natural key
     * @param pollEnabled whether the scheduled and startup entry points should actually run
     */
    public AssetPoller(
        @Qualifier("assetSession") @NotNull JpaSession assetSession,
        @NotNull RefreshTrigger refreshTrigger,
        @NotNull SkyBlockDataContract contract,
        @NotNull LastResponseAccessor lastResponseAccessor,
        @Value("${skyblock.data.github.source-id:skyblock-data}") @NotNull String sourceId,
        @Value("${skyblock.data.github.poll-enabled:true}") boolean pollEnabled
    ) {
        this.assetSession = assetSession;
        this.refreshTrigger = refreshTrigger;
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
     * the next scheduled fire can retry cleanly. Guarded by {@link #pollLock} so the startup
     * and scheduled entry points cannot run in parallel on first boot.
     *
     * @param cause a short tag identifying the trigger ({@code "startup"} or {@code "scheduled"})
     *              purely for log correlation
     */
    private void doPoll(@NotNull String cause) {
        if (!this.pollLock.tryLock()) {
            log.debug(
                "AssetPoller source='{}' - poll already in progress (cause={}), skipping this fire",
                this.sourceId, cause
            );
            return;
        }

        try {
            ExternalAssetState currentState = this.loadCurrentState();
            String storedEtag = currentState.getEtag().orElse(null);
            String storedCommitSha = currentState.getCommitSha().orElse(null);

            Optional<CommitProbe> probe = this.probeLatestCommit();

            if (probe.isEmpty()) {
                this.recordNoChange();
                log.debug(
                    "AssetPoller source='{}' - no change (cache-miss 304 revalidation, storedEtag='{}')",
                    this.sourceId, storedEtag
                );
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
        } finally {
            this.pollLock.unlock();
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
     * Issues the Commits API call and returns the parsed commit sha + ETag header, or
     * {@link Optional#empty()} on a cache-miss {@code 304} revalidation.
     *
     * <p>The Phase 5.5.1 client-library update auto-attaches {@code If-None-Match} from the
     * framework's recent-response cache on every {@code GET}, so this method just makes a
     * plain contract call. Three distinct outcomes are possible:
     * <ul>
     *   <li><b>Fresh 200</b> - GitHub returned new content. The framework caches the response
     *       and the contract returns the new commit list. {@code lastResponseAccessor} exposes
     *       the 200 envelope with the new ETag + body.</li>
     *   <li><b>Transparent 304</b> - GitHub returned {@code 304 Not Modified} and the framework
     *       located a matching cached body in {@code Client.getRecentResponses()}. The response
     *       interceptor synthesizes a new {@code Response.Impl} with
     *       {@link dev.simplified.client.response.HttpStatus#NOT_MODIFIED} and the borrowed
     *       body, appends it to the cache, and returns it to the contract. This method sees a
     *       normal return, reads the synthesized last-response, and extracts the commit sha
     *       from the cached body - which matches the stored sha, so the caller short-circuits
     *       via the {@code equals(storedCommitSha)} check in {@code doPoll}.</li>
     *   <li><b>Cache-miss 304</b> - GitHub returned {@code 304} but the framework has no
     *       matching cached body (first call after restart, TTL eviction, streaming endpoint).
     *       The response interceptor falls through to the error-decoder path, which raises
     *       {@link NotModifiedException}. This method catches the exception and returns empty;
     *       the caller treats this as a no-change cycle and bumps {@code lastCheckedAt}.</li>
     * </ul>
     *
     * @return a {@link CommitProbe} on 200 or transparent 304, or empty on cache-miss 304
     */
    private @NotNull Optional<CommitProbe> probeLatestCommit() {
        try {
            this.contract.getLatestMasterCommit();
        } catch (NotModifiedException ex) {
            return Optional.empty();
        }

        Optional<Response<?>> lastResponse = this.lastResponseAccessor.getLastResponse();

        if (lastResponse.isEmpty())
            throw new IllegalStateException("Framework did not cache a response for getLatestMasterCommit");

        Response<?> response = lastResponse.get();

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
        byte[] bytes = this.contract.getFileContent(MANIFEST_PATH);
        String contentSha = sha256Hex(bytes);
        String rawJson = new String(bytes, StandardCharsets.UTF_8);
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
     * Applies the computed diff to the asset-state tables inside a single transaction,
     * then fires a Phase 5.5 targeted refresh against the SkyBlock session for the models
     * whose underlying files changed.
     *
     * <p>Order of operations inside the transaction:
     * <ol>
     *   <li>Upsert the {@link ExternalAssetState} row with fresh commit sha, ETag, content hash, and timestamps.</li>
     *   <li>Delete every {@link ExternalAssetEntryState} row in {@link AssetDiff#getRemoved()}.</li>
     *   <li>Persist every new entry in {@link AssetDiff#getAdded()}.</li>
     *   <li>Update every changed entry in {@link AssetDiff#getChanged()} to the new {@code content_sha256}.</li>
     * </ol>
     *
     * <p>After the transaction commits, {@link #resolveRefreshTargets(AssetDiff)} reads
     * {@code added + changed} entries and extracts their {@code model_class} FQCNs into a
     * {@code Set<Class<? extends JpaModel>>}. The set is passed to {@link #refreshTrigger}
     * <b>outside</b> the transaction so the downstream GitHub fetches (initiated inside
     * {@link dev.simplified.persistence.source.RemoteJsonSource#load}) do not hold the
     * asset-state connection open across network I/O. A refresh failure is isolated to its
     * own {@code try/catch} so a single bad model does not crash the poll cycle - the next
     * scheduled fire retries.
     *
     * <p>The {@code removed} diff category is deliberately ignored by the refresh trigger:
     * {@link ExternalAssetEntryState} rows carry only {@code entryPath}, not
     * {@code modelClass}, so the mapping from a just-deleted path back to a model class is
     * not recoverable without additional state. A non-empty {@code removed} list emits a
     * {@code WARN} pointing operators at the next container restart as the resolution path;
     * this case is rare in practice (removing an entire entity family is a breaking change).
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

        this.triggerRefresh(diff);
    }

    /**
     * Resolves the Phase 5.5 refresh targets from the diff and fires
     * {@link #refreshTrigger}, isolating any failure so it cannot propagate into the
     * surrounding poll cycle.
     *
     * @param diff the per-entry delta from {@link AssetDiffEngine}
     */
    private void triggerRefresh(@NotNull AssetDiff diff) {
        if (!diff.getRemoved().isEmpty()) {
            log.warn(
                "AssetPoller source='{}' - {} entries removed from manifest; targeted refresh cannot map path back to model class, deferred to next container restart",
                this.sourceId, diff.getRemoved().size()
            );
        }

        Set<Class<? extends JpaModel>> targets = this.resolveRefreshTargets(diff);

        if (targets.isEmpty()) {
            log.debug("AssetPoller source='{}' - no refresh targets resolved from diff", this.sourceId);
            return;
        }

        String targetNames = targets.stream()
            .map(Class::getSimpleName)
            .sorted()
            .collect(Collectors.joining(", "));

        try {
            this.refreshTrigger.refresh(targets);
            log.info(
                "AssetPoller source='{}' - triggered refresh for {} models: [{}]",
                this.sourceId, targets.size(), targetNames
            );
        } catch (RuntimeException ex) {
            log.error(
                "AssetPoller source='{}' - refresh trigger failed for models [{}]: {}",
                this.sourceId, targetNames, ex.getMessage(), ex
            );
        }
    }

    /**
     * Reads {@link AssetDiff#getAdded()} and {@link AssetDiff#getChanged()} and returns the
     * set of {@link JpaModel} classes they reference via {@link ManifestIndex.Entry#getModelClass()}.
     *
     * <p>A {@link ClassNotFoundException} on any entry is logged at {@code WARN} and that
     * entry is skipped. This is the graceful-degradation path for a manifest that
     * references a class which has been renamed or moved since the running container's
     * last deploy - the stable container keeps running with its current schema, and the
     * targeted refresh simply omits the unresolvable entries.
     *
     * <p>Duplicate FQCNs (multiple manifest entries sharing the same model class, or an
     * entry present in both added and changed) collapse into a single set element.
     *
     * @param diff the per-entry delta from {@link AssetDiffEngine}
     * @return the resolved set of model classes, possibly empty
     */
    private @NotNull Set<Class<? extends JpaModel>> resolveRefreshTargets(@NotNull AssetDiff diff) {
        Set<String> fqcns = new LinkedHashSet<>();

        for (ManifestIndex.Entry entry : diff.getAdded())
            fqcns.add(entry.getModelClass());

        for (AssetDiff.ChangedEntry changed : diff.getChanged())
            fqcns.add(changed.getCurrent().getModelClass());

        Set<Class<? extends JpaModel>> targets = new HashSet<>();

        for (String fqcn : fqcns) {
            try {
                Class<?> loaded = Class.forName(fqcn);

                if (!JpaModel.class.isAssignableFrom(loaded)) {
                    log.warn(
                        "AssetPoller source='{}' - manifest model_class '{}' is not a JpaModel, skipping",
                        this.sourceId, fqcn
                    );
                    continue;
                }

                targets.add(loaded.asSubclass(JpaModel.class));
            } catch (ClassNotFoundException ex) {
                log.warn(
                    "AssetPoller source='{}' - manifest references unknown model_class '{}', skipping refresh for this entry",
                    this.sourceId, fqcn
                );
            }
        }

        return targets;
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
