package dev.sbs.simplifieddata.write;

import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.source.WriteRequest;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.UUID;

/**
 * In-memory record of a single buffered write mutation queued inside a
 * {@code WritableRemoteJsonSource} awaiting the next
 * {@link WriteBatchScheduler} tick.
 *
 * <p>Produced by {@link WriteQueueConsumer} when it drains a
 * {@link WriteRequest} off the Hazelcast {@code skyblock.writes} queue (or a
 * {@link RetryEnvelope} from the retry IMap) and dispatches it to a
 * {@code WritableRemoteJsonSource.upsert(T)} or {@code .delete(T)}. Consumed
 * by the matching {@code commitBatch()} or {@code stageBatch()} call on the
 * next scheduler tick.
 *
 * <p>The record carries the hydrated entity (so {@code stageBatch()} can
 * merge it into the current file state without touching the queue again),
 * the {@link Instant} at which the mutation entered the buffer (for end-to-end
 * latency observability), and the {@link #attempt} counter which tracks how
 * many retry rounds the mutation has already been through. A fresh mutation
 * off the primary IQueue has {@code attempt = 0}; a rehydrated retry has
 * {@code attempt = N} where N is the retry cycle the IMap entry was at when
 * it was drained. The scheduler reads this field on escalation and schedules
 * the next retry as {@code attempt + 1}, which cleanly bounds the retry chain
 * at {@code maxRetryAttempts} instead of looping forever.
 *
 * @param <T> the entity type being mutated
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PUBLIC)
public final class BufferedMutation<T extends JpaModel> {

    /** Whether this mutation is an upsert or a delete. */
    private final @NotNull WriteRequest.Operation operation;

    /** The hydrated entity that the mutation applies to. */
    private final @NotNull T entity;

    /** The originating {@link WriteRequest#getRequestId()} for traceability in logs and the dead-letter IMap. */
    private final @NotNull UUID requestId;

    /** The instant the mutation was buffered - used for per-entry latency observability. */
    private final @NotNull Instant bufferedAt;

    /**
     * The retry attempt counter this mutation entered the buffer with.
     *
     * <p>{@code 0} for a fresh mutation dispatched from the primary IQueue.
     * {@code N > 0} when the mutation was rehydrated from the Phase 6b.1
     * {@link RetryEnvelope} IMap on its Nth retry cycle.
     *
     * <p>The scheduler reads this field in
     * {@code WriteBatchScheduler.escalateStagedBatch} and schedules the next
     * retry as {@code attempt + 1}, producing a bounded retry chain that
     * terminates in a dead-letter after {@code maxRetryAttempts} cycles. The
     * field is authoritative for "has this mutation been retried N times
     * already" - do NOT rely on external attempt-tracking maps keyed on
     * {@link #requestId} because those are not preserved across consumer
     * restarts.
     */
    private final int attempt;

    /**
     * Convenience constructor for the common case of a fresh mutation with
     * {@code attempt = 0}. Keeps call sites terse where the attempt counter
     * is known to be zero (producer dispatch, test fixtures).
     *
     * @param operation upsert or delete
     * @param entity the hydrated entity
     * @param requestId the originating producer request id
     * @param bufferedAt the instant the mutation was buffered
     */
    public BufferedMutation(
        @NotNull WriteRequest.Operation operation,
        @NotNull T entity,
        @NotNull UUID requestId,
        @NotNull Instant bufferedAt
    ) {
        this(operation, entity, requestId, bufferedAt, 0);
    }

}
