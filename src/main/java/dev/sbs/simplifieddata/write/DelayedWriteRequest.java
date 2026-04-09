package dev.sbs.simplifieddata.write;

import dev.simplified.persistence.JpaModel;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * A {@link BufferedMutation} tagged with an exponential-backoff retry
 * deadline + target entity class so it can be held in a
 * {@link DelayQueue DelayQueue&lt;DelayedWriteRequest&gt;} until its next
 * retry window opens.
 *
 * <p>Used by {@link WriteQueueConsumer} to track write requests that failed
 * to commit on the first pass (either because the 412 immediate-retry cap
 * was exhausted, the target file was unreachable, or the consumer caught an
 * unexpected {@link Throwable} during dispatch). Each retry doubles the
 * delay starting at {@code initialDelay} (typically 1 minute), capped at
 * {@code maxAttempts} total attempts (typically 5). On the final failed
 * attempt the request is dead-lettered to a dedicated
 * {@code IMap<UUID, WriteRequest> skyblock.writes.deadletter} Hazelcast map
 * for operator inspection.
 *
 * <p>The retry envelope carries the hydrated {@link BufferedMutation} (not
 * a raw {@link dev.simplified.persistence.source.WriteRequest}) so the retry
 * dispatch path skips re-deserialization. The {@link BufferedMutation}'s
 * {@code requestId} preserves the original producer's correlation id across
 * the retry cycle for audit traces.
 *
 * <p>Thread safety: instances are immutable after construction, so the
 * {@link DelayQueue}'s internal ordering is stable without external locking.
 *
 * @see WriteQueueConsumer
 * @see java.util.concurrent.DelayQueue
 */
@Getter
public final class DelayedWriteRequest implements Delayed {

    /** The target entity class - used by the consumer to locate the {@code WritableRemoteJsonSource}. */
    private final @NotNull Class<? extends JpaModel> entityType;

    /** The hydrated mutation, complete with the original producer's request id. */
    private final @NotNull BufferedMutation<?> mutation;

    /**
     * Zero-based retry attempt counter. {@code 1} means the request has been
     * tried once already (the initial attempt that failed) and is now waiting
     * for the first backoff window. The consumer uses this to compute the
     * next delay and to decide when to dead-letter.
     */
    private final int attempt;

    /** The absolute instant at which this request becomes eligible for re-dispatch. */
    private final @NotNull Instant readyAt;

    /**
     * Constructs a delayed retry entry.
     *
     * @param entityType the target entity class
     * @param mutation the hydrated buffered mutation to retry
     * @param attempt the attempt counter (first retry is 1)
     * @param readyAt the absolute instant at which the retry window opens
     */
    public DelayedWriteRequest(
        @NotNull Class<? extends JpaModel> entityType,
        @NotNull BufferedMutation<?> mutation,
        int attempt,
        @NotNull Instant readyAt
    ) {
        this.entityType = entityType;
        this.mutation = mutation;
        this.attempt = attempt;
        this.readyAt = readyAt;
    }

    /**
     * Computes the {@link Instant} at which a retry at the given attempt
     * number (1-based) should become eligible for dispatch, using
     * exponential backoff anchored on the supplied initial delay:
     * {@code initialDelay * 2^(attempt - 1)}.
     *
     * @param now the current instant
     * @param attempt the retry attempt counter (first retry is 1)
     * @param initialDelay the base delay for the first retry
     * @return the absolute instant at which the retry becomes ready
     */
    public static @NotNull Instant readyAtFor(@NotNull Instant now, int attempt, @NotNull Duration initialDelay) {
        long factor = 1L << (attempt - 1);
        Duration backoff = initialDelay.multipliedBy(factor);
        return now.plus(backoff);
    }

    @Override
    public long getDelay(@NotNull TimeUnit unit) {
        long millis = Duration.between(Instant.now(), this.readyAt).toMillis();
        return unit.convert(millis, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(@NotNull Delayed other) {
        return Long.compare(
            this.getDelay(TimeUnit.MILLISECONDS),
            other.getDelay(TimeUnit.MILLISECONDS)
        );
    }

}
