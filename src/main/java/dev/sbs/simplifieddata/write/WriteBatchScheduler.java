package dev.sbs.simplifieddata.write;

import dev.sbs.simplifieddata.persistence.RemoteSkyBlockFactory;
import dev.sbs.simplifieddata.persistence.WritableRemoteJsonSource;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.persistence.JpaModel;
import jakarta.annotation.PreDestroy;
import lombok.extern.log4j.Log4j2;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

/**
 * Spring {@code @Scheduled} component that periodically drains every
 * {@link WritableRemoteJsonSource} in the
 * {@link RemoteSkyBlockFactory#getWritableSources() registry} by invoking
 * each source's {@code commitBatch()} method in turn.
 *
 * <p>Cadence is controlled by {@code skyblock.data.github.write-batch-interval-seconds}
 * (default {@code 10}). A tick that finds every source's buffer empty is a
 * no-op - {@code commitBatch()} returns {@link WritableRemoteJsonSource.CommitBatchResult#empty()}
 * without touching the network.
 *
 * <p>Failure handling: any failed commits reported in
 * {@link WritableRemoteJsonSource.CommitBatchResult#getFailures()} are
 * escalated back to {@link WriteQueueConsumer#scheduleRetry(DelayedWriteRequest)}
 * as {@link DelayedWriteRequest} entries, each carrying the attempt counter
 * and the next readyAt instant computed from the configured initial delay
 * and exponential-backoff multiplier.
 *
 * <p>The retry envelope uses the {@link BufferedMutation}'s captured
 * {@code requestId} as the {@link WriteRequest}'s id so audit traces across
 * the original producer, the consumer dispatch, the failed commit, and the
 * eventual retry all share the same correlation id.
 *
 * <p>Shutdown: {@code @PreDestroy} invokes one final {@code commitBatch()}
 * pass across all sources before the container exits, so in-flight
 * mutations get a last chance to reach GitHub cleanly. This does NOT
 * attempt retries - any failure during shutdown logs a WARN and drops the
 * mutation (the retry queue is torn down in the same lifecycle phase).
 *
 * @see WritableRemoteJsonSource
 * @see WriteQueueConsumer
 */
@Component
@Log4j2
public class WriteBatchScheduler {

    private final @NotNull RemoteSkyBlockFactory factory;
    private final @NotNull WriteQueueConsumer consumer;
    private final @NotNull Duration retryInitialDelay;

    public WriteBatchScheduler(
        @NotNull RemoteSkyBlockFactory remoteSkyBlockFactory,
        @NotNull WriteQueueConsumer writeQueueConsumer,
        @Value("${skyblock.data.github.write-retry-initial-delay-minutes:1}") long retryInitialDelayMinutes
    ) {
        this.factory = remoteSkyBlockFactory;
        this.consumer = writeQueueConsumer;
        this.retryInitialDelay = Duration.ofMinutes(retryInitialDelayMinutes);
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
     * Uses the same commit loop but with retries disabled so a failure here
     * just logs and drops the mutation.
     */
    @PreDestroy
    void flushOnShutdown() {
        log.info("WriteBatchScheduler flushOnShutdown invoked - final commitBatch pass across all sources");
        this.commitAllSources(true);
    }

    /**
     * Iterates the writable source registry and invokes {@code commitBatch}
     * on each source. Failures are escalated to the retry queue (normal
     * ticks) or logged and dropped (shutdown flush).
     *
     * @param isShutdown {@code true} when invoked during shutdown flush
     */
    private void commitAllSources(boolean isShutdown) {
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

            this.escalateFailures(entry.getKey(), result);
        }

        if (totalApplied > 0 || totalFailed > 0) {
            log.info(
                "write batch tick summary: applied={} failed={} (shutdown={})",
                totalApplied, totalFailed, isShutdown
            );
        }
    }

    /**
     * Converts every failed {@link BufferedMutation} in a
     * {@link WritableRemoteJsonSource.CommitBatchResult} into a
     * {@link DelayedWriteRequest} and pushes it onto the consumer's retry
     * queue. Each mutation restarts the attempt counter at 1, because the
     * {@code BufferedMutation} does not carry a prior attempt counter - the
     * source-level immediate 412 retries are separate from the
     * exponential-backoff retry path.
     */
    private void escalateFailures(
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
