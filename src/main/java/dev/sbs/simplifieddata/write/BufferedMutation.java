package dev.sbs.simplifieddata.write;

import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.source.WriteRequest;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;

/**
 * In-memory record of a single buffered write mutation queued inside a
 * {@code WritableRemoteJsonSource} awaiting the next
 * {@link WriteBatchScheduler} tick.
 *
 * <p>Produced by {@link WriteQueueConsumer} when it drains a
 * {@link WriteRequest} off the Hazelcast {@code skyblock.writes} queue and
 * dispatches it to a {@code WritableRemoteJsonSource.upsert(T)} or
 * {@code .delete(T)}. Consumed by the matching
 * {@code commitBatch()} call on the next scheduler tick, which snapshots the
 * per-source buffer, groups by target file, and issues the GitHub Contents API
 * PUT.
 *
 * <p>The record carries both the hydrated entity (so
 * {@code commitBatch()} can merge it into the current file state without
 * touching the queue again) and the {@link Instant} at which the mutation
 * entered the buffer (so observability logs can surface the end-to-end latency
 * from producer put to GitHub commit).
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
    private final @NotNull java.util.UUID requestId;

    /** The instant the mutation was buffered - used for per-entry latency observability. */
    private final @NotNull Instant bufferedAt;

}
