package dev.sbs.simplifieddata.write;

import dev.simplified.persistence.source.WriteRequest;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;

/**
 * Durable retry-state envelope stored in the Hazelcast
 * {@code skyblock.writes.retry} IMap during exponential backoff. Carries the
 * original {@link WriteRequest} (reserialized via the producer's Gson so the
 * envelope is fully self-contained) plus the retry attempt counter and the
 * absolute epoch-millis instant at which the retry becomes eligible for
 * re-dispatch.
 *
 * <p>Phase 6b.1 originally held this state in a local
 * {@code DelayQueue<DelayedWriteRequest>} inside {@link WriteQueueConsumer},
 * which was simple but lost every in-flight retry on consumer restart. The
 * 6b.1 Gap 1 fix migrates the retry state to an {@link com.hazelcast.map.IMap}
 * so a crash or planned restart of {@code simplified-data} no longer drops
 * in-flight retries - the next process starts up, the consumer's drain loop
 * scans the IMap, and any entry whose {@link #readyAtEpochMillis} has already
 * elapsed is immediately processed.
 *
 * <p>This class is plain Java {@link Serializable} with no Hazelcast-specific
 * interfaces. Hazelcast's default serializer handles the envelope via the
 * JDK {@code ObjectOutputStream} path, which works because every field is a
 * JDK value type ({@code int}, {@code long}) or a {@link Serializable}
 * reference ({@link WriteRequest} which is itself plain Java
 * {@code Serializable} per the Phase 6a decision). No custom
 * {@code StreamSerializer} registration is required.
 *
 * @see WriteQueueConsumer
 * @see WriteBatchScheduler
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class RetryEnvelope implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    /** The producer's original write request - entity payload is pre-serialized via Gson on the producer side. */
    private final @NotNull WriteRequest request;

    /**
     * The retry attempt counter this envelope represents. {@code 1} for the
     * first retry (the original dispatch was attempt 0), incrementing on
     * every failed commit cycle. Bounded by
     * {@code skyblock.data.github.write-retry-max-attempts} - once the
     * counter exceeds the cap, the consumer dead-letters the envelope to
     * {@code skyblock.writes.deadletter} instead of putting it back into the
     * retry IMap.
     */
    private final int attempt;

    /**
     * Absolute epoch-millis instant at which this retry becomes eligible for
     * re-dispatch. Stored as {@code long} rather than {@link Instant} to
     * keep the serialized form small and avoid any JDK version drift in the
     * {@code Instant} serial form across the Hazelcast cluster.
     */
    private final long readyAtEpochMillis;

    /**
     * Builds a new retry envelope for the given {@link WriteRequest} at the
     * given attempt counter and absolute ready instant.
     *
     * @param request the write request (with its entity pre-serialized via
     *                the producer's Gson)
     * @param attempt the retry attempt counter this envelope represents
     * @param readyAt the absolute instant at which this retry becomes eligible
     * @return a new envelope
     */
    public static @NotNull RetryEnvelope forRetry(
        @NotNull WriteRequest request,
        int attempt,
        @NotNull Instant readyAt
    ) {
        return new RetryEnvelope(request, attempt, readyAt.toEpochMilli());
    }

    /**
     * Computes the absolute ready instant for the given retry attempt using
     * exponential backoff from {@code initialDelay}. The first retry
     * ({@code attempt = 1}) becomes ready at {@code now + initialDelay}; the
     * Nth retry is ready at {@code now + initialDelay * 2^(N-1)}.
     *
     * <p>Matches the cadence from the original Phase 6b.1
     * {@code DelayedWriteRequest.readyAtFor} helper so the migration
     * preserves the exponential-backoff schedule exactly.
     *
     * @param now the current instant
     * @param attempt the retry attempt counter (first retry is 1)
     * @param initialDelay the base delay for the first retry
     * @return the absolute instant at which the retry becomes ready
     */
    public static @NotNull Instant computeReadyAt(
        @NotNull Instant now,
        int attempt,
        @NotNull Duration initialDelay
    ) {
        long factor = 1L << (attempt - 1);
        Duration backoff = initialDelay.multipliedBy(factor);
        return now.plus(backoff);
    }

    /**
     * Returns {@code true} when this envelope's ready instant has elapsed -
     * i.e. the consumer is allowed to take it out of the retry IMap and
     * re-dispatch the mutation to the owning source.
     *
     * @param nowEpochMillis the current epoch-millis instant
     * @return {@code true} when the retry is eligible for dispatch
     */
    public boolean isReady(long nowEpochMillis) {
        return this.readyAtEpochMillis <= nowEpochMillis;
    }

}
