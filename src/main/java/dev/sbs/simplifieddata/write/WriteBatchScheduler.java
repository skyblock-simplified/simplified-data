package dev.sbs.simplifieddata.write;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import dev.sbs.minecraftapi.MinecraftApi;
import dev.sbs.simplifieddata.persistence.RemoteSkyBlockFactory;
import dev.sbs.simplifieddata.persistence.WritableRemoteJsonSource;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.persistence.JpaModel;
import jakarta.annotation.PreDestroy;
import lombok.extern.log4j.Log4j2;
import org.jetbrains.annotations.NotNull;
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
    private final @NotNull Duration retryInitialDelay;
    private final @NotNull WriteMode writeMode;

    public WriteBatchScheduler(
        @NotNull RemoteSkyBlockFactory remoteSkyBlockFactory,
        @NotNull WriteQueueConsumer writeQueueConsumer,
        @NotNull GitDataCommitService gitDataCommitService,
        @Value("${skyblock.data.github.write-retry-initial-delay-minutes:1}") long retryInitialDelayMinutes,
        @Value("${skyblock.data.github.write-mode:GIT_DATA}") @NotNull WriteMode writeMode
    ) {
        this.factory = remoteSkyBlockFactory;
        this.consumer = writeQueueConsumer;
        this.gitDataCommitService = gitDataCommitService;
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

        Gson gson = MinecraftApi.getGson();

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

        GitDataCommitResult result = this.gitDataCommitService.commit(request);

        if (result.isSuccess()) {
            log.info(
                "Git Data tick summary: applied={} files={} commit={} (shutdown={})",
                request.getTotalMutationCount(), request.getDirtyFileCount(), result.getCommitSha(), isShutdown
            );
            return;
        }

        if (isShutdown) {
            log.warn(
                "Git Data tick failed during shutdown flush - dropping {} mutations across {} files",
                request.getTotalMutationCount(), request.getDirtyFileCount()
            );
            return;
        }

        // Escalate every mutation in every source batch.
        for (StagedBatch<?> sourceBatch : request.getSourceBatches())
            this.escalateStagedBatch(sourceBatch.getModelClass(), sourceBatch);

        log.warn(
            "Git Data tick escalated {} mutations across {} files to retry queue",
            request.getTotalMutationCount(), request.getDirtyFileCount()
        );
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

            try {
                result = source.commitBatch();
            } catch (Throwable ex) {
                log.error(
                    "commitBatch threw unexpected exception for type '{}' - continuing with next source",
                    entry.getKey().getSimpleName(), ex
                );
                continue;
            }

            if (result.isEmpty())
                continue;

            if (result.isSuccess()) {
                totalApplied += result.getAppliedCount();
                continue;
            }

            totalFailed += result.getFailures().size();

            if (isShutdown) {
                log.warn(
                    "commitBatch failed during shutdown flush for type '{}' - dropping {} mutations",
                    entry.getKey().getSimpleName(), result.getFailures().size()
                );
                continue;
            }

            this.escalateCommitFailures(entry.getKey(), result);
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
     * {@link DelayedWriteRequest} with {@code attempt=1} and pushes each
     * entry onto the consumer's retry queue. Used by the Git Data failure
     * escalation path.
     */
    private void escalateStagedBatch(
        @NotNull Class<? extends JpaModel> type,
        @NotNull StagedBatch<?> staged
    ) {
        Instant now = Instant.now();

        for (BufferedMutation<?> mutation : staged.getMutationsUntyped()) {
            int attempt = 1;
            Instant readyAt = DelayedWriteRequest.readyAtFor(now, attempt, this.retryInitialDelay);
            this.consumer.scheduleRetry(new DelayedWriteRequest(type, mutation, attempt, readyAt));
        }
    }

    /**
     * Converts every failed {@link BufferedMutation} in a
     * {@link WritableRemoteJsonSource.CommitBatchResult} into a
     * {@link DelayedWriteRequest} and pushes it onto the consumer's retry
     * queue. Used by the Contents API failure escalation path.
     */
    private void escalateCommitFailures(
        @NotNull Class<? extends JpaModel> type,
        @NotNull WritableRemoteJsonSource.CommitBatchResult result
    ) {
        Instant now = Instant.now();

        for (BufferedMutation<?> mutation : result.getFailures()) {
            int attempt = 1;
            Instant readyAt = DelayedWriteRequest.readyAtFor(now, attempt, this.retryInitialDelay);
            this.consumer.scheduleRetry(new DelayedWriteRequest(type, mutation, attempt, readyAt));
        }

        log.warn(
            "Escalated {} failed mutations for type '{}' to retry queue",
            result.getFailures().size(), type.getSimpleName()
        );
    }

}
