package dev.sbs.simplifieddata.write;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import dev.sbs.simplifieddata.DataApi;
import dev.sbs.simplifieddata.config.GitHubConfig;
import dev.sbs.simplifieddata.persistence.RemoteSkyBlockFactory;
import dev.sbs.simplifieddata.persistence.WritableRemoteJsonSource;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.source.WriteRequest;
import jakarta.annotation.PreDestroy;
import lombok.extern.log4j.Log4j2;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.lang.reflect.Type;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

/**
 * Spring {@code @Scheduled} component that periodically drains every
 * {@link WritableRemoteJsonSource} in the
 * {@link RemoteSkyBlockFactory#getWritableSources() registry} and commits
 * the drained state to GitHub via one of two paths, selected by the
 * {@code skyblock.data.github.write-mode} property:
 *
 * <ul>
 *   <li><b>{@link WriteMode#GIT_DATA} (default)</b>: the Phase 6b.1
 *       two-phase commit path. Phase 1 calls
 *       {@link WritableRemoteJsonSource#stageBatch()} on every dirty source
 *       to accumulate a cross-source {@link BatchCommitRequest}; phase 2
 *       hands the merged request to {@link GitDataCommitService#commit}
 *       which runs the 7-step Git Data API flow and produces ONE commit
 *       per tick across every dirty file on every source. The
 *       {@code items.json}/{@code items_extra.json} dual-file routing
 *       works correctly here because staging reads both files and
 *       dispatches by id ownership; the commit atomically updates both
 *       files in a single git tree.</li>
 *   <li><b>{@link WriteMode#CONTENTS}</b>: the Phase 6b fallback path.
 *       Iterates sources and calls
 *       {@link WritableRemoteJsonSource#commitBatch()} on each, producing
 *       one GitHub commit per dirty source per tick via the Contents API.
 *       Retained as an operational fallback if the Git Data API path
 *       becomes unhealthy.</li>
 * </ul>
 *
 * <p>Cadence is controlled by {@code skyblock.data.github.write-batch-interval-seconds}
 * (default {@code 10}). A tick that finds every source's buffer empty is a
 * no-op in both modes.
 *
 * <p>Failure handling is uniform across modes: any failed mutation is
 * re-queued via
 * {@link WriteQueueConsumer#scheduleRetry(DelayedWriteRequest)} with
 * {@code attempt=1} and a {@code readyAt} instant computed from the
 * configured initial delay. The retry envelope carries the hydrated
 * {@link BufferedMutation} so re-dispatch skips deserialization and
 * preserves the original producer's request id.
 *
 * <p>Shutdown: {@code @PreDestroy} invokes one final commit pass with
 * retries disabled so any in-flight mutations get a last chance to reach
 * GitHub cleanly. Any failure during shutdown logs a WARN and drops the
 * mutation (the retry queue is torn down in the same lifecycle phase).
 *
 * @see WritableRemoteJsonSource
 * @see GitDataCommitService
 * @see WriteQueueConsumer
 * @see WriteMode
 */
@Component
@Log4j2
public class WriteBatchScheduler {

    private final @NotNull RemoteSkyBlockFactory factory;
    private final @NotNull WriteQueueConsumer consumer;
    private final @NotNull GitDataCommitService gitDataCommitService;
    private final @NotNull WriteMetrics metrics;
    private final @NotNull Duration retryInitialDelay;
    private final @NotNull WriteMode writeMode;

    public WriteBatchScheduler(
        @NotNull RemoteSkyBlockFactory remoteSkyBlockFactory,
        @NotNull WriteQueueConsumer writeQueueConsumer,
        @NotNull GitDataCommitService gitDataCommitService,
        @NotNull WriteMetrics writeMetrics,
        @Value("${skyblock.data.github.write-retry-initial-delay-minutes:1}") long retryInitialDelayMinutes,
        @Value("${skyblock.data.github.write-mode:GIT_DATA}") @NotNull WriteMode writeMode
    ) {
        this.factory = remoteSkyBlockFactory;
        this.consumer = writeQueueConsumer;
        this.gitDataCommitService = gitDataCommitService;
        this.metrics = writeMetrics;
        this.retryInitialDelay = Duration.ofMinutes(retryInitialDelayMinutes);
        this.writeMode = writeMode;
    }

    /**
     * Scheduled entry point. Fires every
     * {@code skyblock.data.github.write-batch-interval-seconds} seconds
     * (default 10) using {@code fixedDelay} semantics so a slow tick never
     * overlaps the next one.
     */
    @Scheduled(fixedDelayString = "${skyblock.data.github.write-batch-interval-seconds:10}000")
    public void tick() {
        this.commitAllSources(false);
    }

    /**
     * Final shutdown flush. Invoked by Spring's {@code @PreDestroy} chain.
     * Uses the same commit dispatch but with retries disabled so a failure
     * here just logs and drops the mutation.
     */
    @PreDestroy
    void flushOnShutdown() {
        log.info("WriteBatchScheduler flushOnShutdown invoked - final commit pass across all sources (mode={})", this.writeMode);
        this.commitAllSources(true);
    }

    /**
     * Dispatches the current tick to the appropriate write-mode handler.
     */
    private void commitAllSources(boolean isShutdown) {
        switch (this.writeMode) {
            case GIT_DATA -> this.commitViaGitData(isShutdown);
            case CONTENTS -> this.commitViaContents(isShutdown);
        }
    }

    /**
     * Phase 6b.1 Git Data API two-phase commit path.
     *
     * <p>Phase A stages every source (no network writes). Phase B merges
     * the staged results into a single {@link BatchCommitRequest} and
     * hands it to {@link GitDataCommitService}. Failures escalate every
     * mutation in every staged source batch to the consumer's retry queue.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private void commitViaGitData(boolean isShutdown) {
        ConcurrentMap<Class<? extends JpaModel>, WritableRemoteJsonSource<?>> sources = this.factory.getWritableSources();
        BatchCommitRequest.Builder builder = BatchCommitRequest.builder();

        Gson gson = DataApi.getGson();

        // Phase A: stage every source into the merged request.
        for (Map.Entry<Class<? extends JpaModel>, WritableRemoteJsonSource<?>> entry : sources.entrySet()) {
            WritableRemoteJsonSource<?> source = entry.getValue();
            StagedBatch<?> staged;

            try {
                staged = source.stageBatch();
            } catch (Throwable ex) {
                log.error(
                    "stageBatch threw unexpected exception for type '{}' - continuing with next source",
                    entry.getKey().getSimpleName(), ex
                );
                continue;
            }

            if (staged.isEmpty())
                continue;

            // The staged batch may be empty-dirty-files but carry mutations that need escalation
            // (the source returned StagedBatch with file snapshots empty because it failed internally).
            if (staged.getFileSnapshots().isEmpty() && !staged.getMutations().isEmpty()) {
                if (isShutdown) {
                    log.warn(
                        "stageBatch produced {} mutations with no dirty files for type '{}' during shutdown - dropping",
                        staged.getMutations().size(), entry.getKey().getSimpleName()
                    );
                } else {
                    this.escalateStagedBatch(entry.getKey(), staged);
                }
                continue;
            }

            Type listType = TypeToken.getParameterized(ConcurrentList.class, entry.getKey()).getType();
            builder.add(staged, entities -> gson.toJson(entities, listType));
        }

        BatchCommitRequest request = builder.build();

        if (request.isEmpty())
            return;

        io.micrometer.core.instrument.Timer.Sample commitSample = this.metrics.recordCommitStart();
        GitDataCommitResult result = this.gitDataCommitService.commit(request);

        if (result.isSuccess()) {
            this.metrics.recordCommitSuccess(commitSample, WriteMetrics.CommitMode.GIT_DATA);
            // Phase 6b.3 end-to-end latency: for every mutation that landed in this
            // tick's commit, record the duration from when it was buffered to now.
            // Measures producer -> GitHub latency per individual mutation, useful
            // for p95/p99 histograms on dashboards.
            for (StagedBatch<?> sourceBatch : request.getSourceBatches()) {
                for (BufferedMutation<?> mutation : sourceBatch.getMutationsUntyped())
                    this.metrics.recordEndToEndLatency(mutation.getBufferedAt());
            }
            log.info(
                "Git Data tick summary: applied={} files={} commit={} (shutdown={})",
                request.getTotalMutationCount(), request.getDirtyFileCount(), result.getCommitSha(), isShutdown
            );
            return;
        }

        // Derive a bounded-enum failure reason from the commit result's root cause.
        WriteMetrics.FailureReason reason = deriveFailureReason(result.getFailureCause());
        this.metrics.recordCommitFailure(commitSample, WriteMetrics.CommitMode.GIT_DATA, reason);

        if (isShutdown) {
            log.warn(
                "Git Data tick failed during shutdown flush - dropping {} mutations across {} files",
                request.getTotalMutationCount(), request.getDirtyFileCount()
            );
            return;
        }

        // Escalate every mutation in every source batch.
        int escalated = 0;
        for (StagedBatch<?> sourceBatch : request.getSourceBatches()) {
            this.escalateStagedBatch(sourceBatch.getModelClass(), sourceBatch);
            escalated += sourceBatch.getMutationsUntyped().size();
        }
        this.metrics.recordEscalation(WriteMetrics.CommitMode.GIT_DATA, escalated);

        log.warn(
            "Git Data tick escalated {} mutations across {} files to retry queue",
            request.getTotalMutationCount(), request.getDirtyFileCount()
        );
    }

    /**
     * Maps a commit failure's root cause to a bounded
     * {@link WriteMetrics.FailureReason} tag value. Uses the
     * {@link dev.sbs.simplifieddata.client.exception.SkyBlockDataException}
     * status code when available, and falls back to coarse classification
     * (network / other) for everything else.
     */
    private static @NotNull WriteMetrics.FailureReason deriveFailureReason(@Nullable Throwable cause) {
        if (cause instanceof dev.sbs.simplifieddata.client.exception.SkyBlockDataException ex)
            return WriteMetrics.FailureReason.fromStatus(ex.getStatus().getCode());
        if (cause instanceof java.net.SocketException || cause instanceof java.net.UnknownHostException)
            return WriteMetrics.FailureReason.NETWORK;
        return WriteMetrics.FailureReason.OTHER;
    }

    /**
     * Phase 6b Contents API fallback path. Iterates the source registry
     * and invokes {@link WritableRemoteJsonSource#commitBatch()} on each
     * dirty source, escalating per-source failures to the retry queue.
     * Kept verbatim from the Phase 6b shipping implementation so operators
     * can flip to this mode via the {@code write-mode=CONTENTS} property
     * if the Git Data API path becomes unhealthy.
     */
    private void commitViaContents(boolean isShutdown) {
        ConcurrentMap<Class<? extends JpaModel>, WritableRemoteJsonSource<?>> sources = this.factory.getWritableSources();
        int totalApplied = 0;
        int totalFailed = 0;

        for (Map.Entry<Class<? extends JpaModel>, WritableRemoteJsonSource<?>> entry : sources.entrySet()) {
            WritableRemoteJsonSource<?> source = entry.getValue();
            WritableRemoteJsonSource.CommitBatchResult result;

            // Per-source Timer.Sample: Contents API mode produces one commit per
            // dirty source per tick, so the timing unit is a single source's
            // commitBatch() call rather than a cross-source aggregation.
            io.micrometer.core.instrument.Timer.Sample commitSample = this.metrics.recordCommitStart();

            try {
                result = source.commitBatch();
            } catch (Throwable ex) {
                this.metrics.recordCommitFailure(commitSample, WriteMetrics.CommitMode.CONTENTS, WriteMetrics.FailureReason.OTHER);
                log.error(
                    "commitBatch threw unexpected exception for type '{}' - continuing with next source",
                    entry.getKey().getSimpleName(), ex
                );
                continue;
            }

            if (result.isEmpty()) {
                // No-op tick for this source: discard the started sample by recording
                // it as a success with zero elapsed. Micrometer has no public API to
                // abandon a sample without stopping it, so we stop with a success
                // marker and the tiny elapsed time is a true reflection of the no-op.
                this.metrics.recordCommitSuccess(commitSample, WriteMetrics.CommitMode.CONTENTS);
                continue;
            }

            if (result.isSuccess()) {
                this.metrics.recordCommitSuccess(commitSample, WriteMetrics.CommitMode.CONTENTS);
                totalApplied += result.getAppliedCount();
                continue;
            }

            this.metrics.recordCommitFailure(commitSample, WriteMetrics.CommitMode.CONTENTS, WriteMetrics.FailureReason.OTHER);
            totalFailed += result.getFailures().size();

            if (isShutdown) {
                log.warn(
                    "commitBatch failed during shutdown flush for type '{}' - dropping {} mutations",
                    entry.getKey().getSimpleName(), result.getFailures().size()
                );
                continue;
            }

            this.escalateCommitFailures(entry.getKey(), result);
            this.metrics.recordEscalation(WriteMetrics.CommitMode.CONTENTS, result.getFailures().size());
        }

        if (totalApplied > 0 || totalFailed > 0) {
            log.info(
                "Contents tick summary: applied={} failed={} (shutdown={})",
                totalApplied, totalFailed, isShutdown
            );
        }
    }

    /**
     * Converts every buffered mutation inside a {@link StagedBatch} into a
     * {@link RetryEnvelope} with {@code attempt = mutation.getAttempt() + 1}
     * and pushes each envelope onto the consumer's durable retry IMap via
     * {@link WriteQueueConsumer#scheduleRetry(RetryEnvelope)}. Used by the
     * Git Data failure escalation path.
     *
     * <p>Reading {@code mutation.getAttempt()} (rather than hardcoding
     * {@code attempt=1}) is the Phase 6b.1 Gap 1 fix: a mutation that was
     * itself a retry (attempt=N) escalates to attempt=N+1, which terminates
     * correctly at {@code maxRetryAttempts} instead of looping forever
     * through a chain of "first retries".
     */
    private void escalateStagedBatch(
        @NotNull Class<? extends JpaModel> type,
        @NotNull StagedBatch<?> staged
    ) {
        Instant now = Instant.now();

        for (BufferedMutation<?> mutation : staged.getMutationsUntyped())
            this.consumer.scheduleRetry(this.buildRetryEnvelope(type, mutation, now));
    }

    /**
     * Converts every failed {@link BufferedMutation} in a
     * {@link WritableRemoteJsonSource.CommitBatchResult} into a
     * {@link RetryEnvelope} and pushes it onto the consumer's retry IMap.
     * Used by the Contents API failure escalation path.
     */
    private void escalateCommitFailures(
        @NotNull Class<? extends JpaModel> type,
        @NotNull WritableRemoteJsonSource.CommitBatchResult result
    ) {
        Instant now = Instant.now();

        for (BufferedMutation<?> mutation : result.getFailures())
            this.consumer.scheduleRetry(this.buildRetryEnvelope(type, mutation, now));

        log.warn(
            "Escalated {} failed mutations for type '{}' to retry IMap",
            result.getFailures().size(), type.getSimpleName()
        );
    }

    /**
     * Rebuilds a {@link WriteRequest} envelope from a failed
     * {@link BufferedMutation}, wraps it in a {@link RetryEnvelope} with
     * {@code attempt = mutation.getAttempt() + 1}, and computes the
     * exponential-backoff ready instant via
     * {@link RetryEnvelope#computeReadyAt(Instant, int, Duration)}.
     *
     * <p>The original producer request id is preserved byte-identically
     * because {@link BufferedMutation#getRequestId()} carries the id from
     * the initial dispatch forward through every retry cycle. Dead-letter
     * queries keyed on requestId correlate end-to-end across the producer
     * put, the initial consumer dispatch, every retry attempt, and the
     * eventual dead-letter entry.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private @NotNull RetryEnvelope buildRetryEnvelope(
        @NotNull Class<? extends JpaModel> type,
        @NotNull BufferedMutation<?> mutation,
        @NotNull Instant now
    ) {
        int nextAttempt = mutation.getAttempt() + 1;
        Instant readyAt = RetryEnvelope.computeReadyAt(now, nextAttempt, this.retryInitialDelay);

        WriteRequest request = switch (mutation.getOperation()) {
            case UPSERT -> WriteRequest.upsert(
                (Class) type,
                (JpaModel) mutation.getEntity(),
                DataApi.getGson(),
                GitHubConfig.SOURCE_ID
            );
            case DELETE -> WriteRequest.delete(
                (Class) type,
                (JpaModel) mutation.getEntity(),
                DataApi.getGson(),
                GitHubConfig.SOURCE_ID
            );
        };
        // Producer-side WriteRequest.upsert/delete generate a fresh UUID for
        // each call, but for retries we need to preserve the original
        // producer's request id so dead-letter and audit trails stay
        // correlated end-to-end across every retry cycle.
        request = request.withRequestId(mutation.getRequestId());
        return RetryEnvelope.forRetry(request, nextAttempt, readyAt);
    }

}
