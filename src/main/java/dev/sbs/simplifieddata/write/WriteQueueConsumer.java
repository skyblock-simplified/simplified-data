package dev.sbs.simplifieddata.write;

import com.google.gson.Gson;
import com.hazelcast.collection.IQueue;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import dev.sbs.simplifieddata.DataApi;
import dev.sbs.simplifieddata.persistence.RemoteSkyBlockFactory;
import dev.sbs.simplifieddata.persistence.WritableRemoteJsonSource;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.source.WriteRequest;
import jakarta.annotation.PreDestroy;
import lombok.extern.log4j.Log4j2;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Drains the {@code skyblock.writes} Hazelcast {@link IQueue} plus the
 * {@code skyblock.writes.retry} Hazelcast {@link IMap} on a dedicated daemon
 * thread, dispatching each drained {@link WriteRequest} into the per-entity
 * {@link WritableRemoteJsonSource} buffer owned by
 * {@link RemoteSkyBlockFactory#getWritableSources()}.
 *
 * <p>Started by an {@code @EventListener(ApplicationReadyEvent)} handler so
 * the drain begins once Spring finishes refreshing the context (same pattern
 * as {@link dev.sbs.simplifieddata.poller.AssetPoller}). Shutdown is managed
 * by {@code @PreDestroy} which interrupts the daemon thread and waits briefly
 * for clean exit.
 *
 * <p>Drain loop: each iteration polls the Hazelcast queue and the retry IMap
 * alternately. The IQueue poll uses a short 500ms timeout so a steady stream
 * of fresh writes does not starve the retry scan, and the retry scan runs
 * opportunistically whenever the queue poll returns empty. For each drained
 * request the consumer:
 * <ol>
 *   <li>Resolves the entity class via {@link WriteRequest#resolveEntityType()},
 *       wrapping {@link ClassNotFoundException} as {@link JpaException} and
 *       logging a WARN.</li>
 *   <li>Looks up the matching
 *       {@link WritableRemoteJsonSource} in
 *       {@link RemoteSkyBlockFactory#getWritableSources()} - missing sources
 *       log a WARN and skip.</li>
 *   <li>Rehydrates the entity via
 *       {@link WriteRequest#deserializeEntity(Gson, Class)}.</li>
 *   <li>Buffers a fresh {@link BufferedMutation} into the target source,
 *       carrying the attempt counter from the retry envelope (or {@code 0}
 *       for fresh IQueue requests) so the scheduler can correctly compute
 *       the next retry on a subsequent failure.</li>
 *   <li>Catches any {@link Throwable} from the body, logs ERROR with the
 *       full {@link WriteRequest#getRequestId()} for traceability, and
 *       continues the loop so a single bad request cannot stop the drain.</li>
 * </ol>
 *
 * <p>Exponential-backoff retry path (Phase 6b.1 Gap 1 fix): failed commits
 * are pushed onto the {@code skyblock.writes.retry} Hazelcast IMap as
 * {@link RetryEnvelope} entries keyed on the original producer request id.
 * Each entry carries the attempt counter and the absolute ready-at instant
 * computed via {@link RetryEnvelope#computeReadyAt(Instant, int, Duration)}
 * exponential backoff. The drain loop scans the IMap on every iteration for
 * entries whose {@code readyAt} has elapsed, atomically removes them, and
 * re-dispatches. Crash/restart durability: pre-existing entries in the IMap
 * from the previous process's lifetime are picked up automatically on the
 * first scan iteration, so in-flight retries survive a clean restart or a
 * crash without being lost.
 *
 * <p>After the retry attempt counter exceeds {@code maxRetryAttempts}, the
 * request is dead-lettered to {@code IMap<UUID, WriteRequest> skyblock.writes.deadletter}
 * for operator inspection. The dead-letter map is auto-created on first
 * access.
 *
 * <p>The consumer does NOT call {@code commitBatch} itself - the
 * {@link WriteBatchScheduler} owns that responsibility on its 10s cadence.
 * Failed commits that come back from the scheduler are surfaced to this
 * consumer via {@link #scheduleRetry(RetryEnvelope)}.
 *
 * @see WriteBatchScheduler
 * @see WritableRemoteJsonSource
 * @see RetryEnvelope
 */
@Component
@Log4j2
public class WriteQueueConsumer {

    /** The Hazelcast IQueue name declared in {@code infra/hazelcast/hazelcast.xml}. */
    public static final @NotNull String QUEUE_NAME = "skyblock.writes";

    /**
     * The Hazelcast IMap name used for the Phase 6b.1 durable retry state.
     * Auto-created on first access. Survives consumer restarts so in-flight
     * exponential-backoff retries are not lost on a crash or redeploy.
     */
    public static final @NotNull String RETRY_MAP_NAME = "skyblock.writes.retry";

    /** The Hazelcast IMap name used for the dead-letter dump. Auto-created on first access. */
    public static final @NotNull String DEADLETTER_MAP_NAME = "skyblock.writes.deadletter";

    private final @NotNull HazelcastInstance writeHazelcastInstance;
    private final @NotNull RemoteSkyBlockFactory factory;
    private final @NotNull WriteMetrics metrics;
    private final @NotNull Gson gson;
    private final int maxRetryAttempts;
    private final boolean enabled;

    private volatile Thread drainThread;
    private volatile boolean stopping = false;

    public WriteQueueConsumer(
        @NotNull HazelcastInstance skyBlockWriteHazelcastInstance,
        @NotNull RemoteSkyBlockFactory remoteSkyBlockFactory,
        @NotNull WriteMetrics writeMetrics,
        @Value("${skyblock.data.github.write-retry-initial-delay-minutes:1}") long retryInitialDelayMinutes,
        @Value("${skyblock.data.github.write-retry-max-attempts:5}") int maxRetryAttempts,
        @Value("${skyblock.data.github.write-consumer-enabled:true}") boolean enabled
    ) {
        this.writeHazelcastInstance = skyBlockWriteHazelcastInstance;
        this.factory = remoteSkyBlockFactory;
        this.metrics = writeMetrics;
        this.gson = DataApi.getGson();
        // retryInitialDelayMinutes is retained as a constructor parameter for
        // backwards-compatible property binding; the actual backoff math lives in
        // WriteBatchScheduler which owns the escalation call site and has the
        // Duration ready when it invokes scheduleRetry.
        this.maxRetryAttempts = maxRetryAttempts;
        this.enabled = enabled;
    }

    /**
     * Starts the drain thread once the Spring context is fully up. Fires
     * at most once per context refresh.
     */
    @EventListener(ApplicationReadyEvent.class)
    public synchronized void start() {
        if (!this.enabled) {
            log.info("WriteQueueConsumer disabled (skyblock.data.github.write-consumer-enabled=false)");
            return;
        }

        if (this.drainThread != null)
            return;

        // Log the retry IMap size so operators can see leftover entries from the previous
        // process's lifetime (durable-restart resumes processing those on the first scan).
        IMap<UUID, RetryEnvelope> retryMap = this.writeHazelcastInstance.getMap(RETRY_MAP_NAME);
        int leftover = retryMap.size();

        if (leftover > 0)
            log.info("WriteQueueConsumer resuming with {} leftover retry entries from prior process lifetime", leftover);

        // Phase 6b.3: register the 3 Hazelcast-backed gauges now that the
        // cluster handshake has completed. The existing drainThread != null
        // guard above ensures start() is idempotent per context refresh, so
        // these registrations only fire once.
        this.metrics.registerRetryImapSizeGauge(this.writeHazelcastInstance);
        this.metrics.registerDeadletterImapSizeGauge(this.writeHazelcastInstance);
        this.metrics.registerPrimaryQueueSizeGauge(this.writeHazelcastInstance);

        Thread thread = new Thread(this::drainLoop, "skyblock-write-consumer");
        thread.setDaemon(true);
        this.drainThread = thread;
        thread.start();
        log.info("WriteQueueConsumer daemon thread started (queue='{}' retryMap='{}')", QUEUE_NAME, RETRY_MAP_NAME);
    }

    /**
     * Signals the drain thread to exit and joins briefly. Invoked on Spring
     * context teardown.
     */
    @PreDestroy
    public synchronized void stop() {
        this.stopping = true;

        Thread thread = this.drainThread;

        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(Duration.ofSeconds(2).toMillis());
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Pushes a failed write back onto the durable retry IMap. Called by
     * {@link WriteBatchScheduler} for each failed mutation returned by a
     * per-source commit tick.
     *
     * <p>If the envelope's attempt counter has already exceeded
     * {@code maxRetryAttempts}, the envelope is dead-lettered immediately
     * instead of entering the retry IMap. Otherwise the envelope is stored
     * under its original {@link WriteRequest#getRequestId() requestId} so
     * subsequent retries overwrite any in-flight entry for the same producer
     * request (dedup on id).
     *
     * @param envelope the retry entry, carrying the WriteRequest, attempt counter,
     *                 and absolute ready instant
     */
    public void scheduleRetry(@NotNull RetryEnvelope envelope) {
        if (envelope.getAttempt() > this.maxRetryAttempts) {
            this.deadLetter(envelope);
            return;
        }

        IMap<UUID, RetryEnvelope> retryMap = this.writeHazelcastInstance.getMap(RETRY_MAP_NAME);
        retryMap.put(envelope.getRequest().getRequestId(), envelope);

        log.info(
            "scheduled retry for WriteRequest {} attempt={} readyAtEpochMillis={}",
            envelope.getRequest().getRequestId(),
            envelope.getAttempt(),
            envelope.getReadyAtEpochMillis()
        );
    }

    /**
     * The drain loop. Runs on the dedicated daemon thread. Iterates until
     * {@link #stopping} is set or the thread is interrupted.
     */
    private void drainLoop() {
        log.info("WriteQueueConsumer drain loop entered");
        IQueue<WriteRequest> queue = this.writeHazelcastInstance.getQueue(QUEUE_NAME);

        while (!this.stopping && !Thread.currentThread().isInterrupted()) {
            try {
                // 1. Prefer fresh writes from the primary queue - short poll so retry scans don't starve.
                WriteRequest fresh = queue.poll(500, TimeUnit.MILLISECONDS);
                if (fresh != null) {
                    this.metrics.recordFreshDispatch();
                    this.metrics.recordDispatchLatency(fresh.getTimestamp());
                    this.dispatch(fresh, 0);
                    continue;
                }

                // 2. Opportunistically scan the retry IMap for ready entries.
                this.drainReadyRetries();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                break;
            } catch (Throwable ex) {
                log.error("WriteQueueConsumer drain loop caught unexpected exception - continuing", ex);
            }
        }

        log.info("WriteQueueConsumer drain loop exited");
    }

    /**
     * Scans the retry IMap once for entries whose {@code readyAt} has
     * elapsed, atomically removes each eligible entry, and dispatches the
     * embedded {@link WriteRequest} through the common dispatch path.
     *
     * <p>The scan uses {@link IMap#keySet()} to snapshot the current key set
     * (cheap for the small retry map sizes expected in practice - at most a
     * few tens of entries during sustained failure conditions), then checks
     * each entry's {@code readyAt} via a per-key {@link IMap#get(Object)}.
     * An {@code IMap.remove(key)} returning non-null confirms we won the
     * race against any other consumer that might also be draining the map
     * (currently a single-writer architecture, but the pattern is
     * race-safe for future multi-consumer scale-out).
     */
    private void drainReadyRetries() {
        IMap<UUID, RetryEnvelope> retryMap = this.writeHazelcastInstance.getMap(RETRY_MAP_NAME);

        if (retryMap.isEmpty())
            return;

        long now = System.currentTimeMillis();
        List<UUID> keys = new ArrayList<>(retryMap.keySet());

        for (UUID key : keys) {
            RetryEnvelope peeked = retryMap.get(key);

            if (peeked == null || !peeked.isReady(now))
                continue;

            RetryEnvelope taken = retryMap.remove(key);

            if (taken == null)
                continue;

            this.metrics.recordRetryDispatch(taken.getAttempt());
            this.dispatch(taken.getRequest(), taken.getAttempt());
        }
    }

    /**
     * Unified dispatch path for both fresh IQueue requests ({@code attempt=0})
     * and retry IMap entries ({@code attempt=N}). Resolves the entity class,
     * rehydrates the entity via Gson, and buffers a {@link BufferedMutation}
     * into the matching source with the supplied attempt counter.
     *
     * <p>Dispatch-time failures (unknown class, non-JpaModel, malformed JSON,
     * missing source registration) log WARN and skip - these conditions do
     * not heal with time, so retrying would loop forever. The
     * {@link BufferedMutation#getAttempt() attempt} counter is propagated
     * into the source buffer so the scheduler's escalation path can compute
     * the next retry as {@code attempt + 1}, producing a bounded retry
     * chain that terminates in a dead-letter after {@code maxRetryAttempts}
     * cycles.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void dispatch(@NotNull WriteRequest request, int attempt) {
        try {
            Class<? extends JpaModel> type = request.resolveEntityType();
            WritableRemoteJsonSource source = this.factory.getWritableSources().get(type);

            if (source == null) {
                this.metrics.recordSkipped(WriteMetrics.SkipReason.MISSING_SOURCE);
                log.warn(
                    "No writable source registered for type '{}' - skipping WriteRequest {}",
                    type.getName(), request.getRequestId()
                );
                return;
            }

            JpaModel entity = request.deserializeEntity(this.gson, type);
            BufferedMutation<JpaModel> mutation = new BufferedMutation<>(
                request.getOperation(), entity, request.getRequestId(), Instant.now(), attempt
            );
            source.buffer(mutation);

            if (attempt == 0) {
                log.info(
                    "consumed WriteRequest {} op={} type={}",
                    request.getRequestId(), request.getOperation(), type.getSimpleName()
                );
            } else {
                log.info(
                    "re-buffered retry WriteRequest {} op={} type={} attempt={}",
                    request.getRequestId(), request.getOperation(), type.getSimpleName(), attempt
                );
            }
        } catch (JpaException ex) {
            // JpaException wraps either ClassNotFoundException (unknown type) or
            // Gson JsonSyntaxException (malformed JSON) inside the resolve/deserialize
            // helpers. The cause type discriminates which skip-reason tag to emit.
            WriteMetrics.SkipReason reason = ex.getCause() instanceof ClassNotFoundException
                ? WriteMetrics.SkipReason.UNKNOWN_TYPE
                : WriteMetrics.SkipReason.MALFORMED_JSON;
            this.metrics.recordSkipped(reason);
            log.warn(
                "Skipping WriteRequest {} - unable to resolve or buffer: {}",
                request.getRequestId(), ex.getMessage()
            );
        } catch (Throwable ex) {
            log.error("Unexpected error dispatching WriteRequest {}", request.getRequestId(), ex);
        }
    }

    /**
     * Pushes a fully-exhausted retry onto the dead-letter IMap for operator
     * inspection. The dead-letter map is keyed on the original producer
     * request id so operator queries line up with audit logs from the
     * initial producer put.
     */
    private void deadLetter(@NotNull RetryEnvelope envelope) {
        try {
            UUID key = envelope.getRequest().getRequestId();
            IMap<UUID, WriteRequest> deadletter = this.writeHazelcastInstance.getMap(DEADLETTER_MAP_NAME);
            deadletter.put(key, envelope.getRequest());
            this.metrics.recordDeadLetter(envelope.getRequest().getEntityClassName());
            log.error(
                "DEAD-LETTER WriteRequest {} op={} type={} after {} attempts - requires operator attention",
                key,
                envelope.getRequest().getOperation(),
                envelope.getRequest().getEntityClassName(),
                envelope.getAttempt() - 1
            );
        } catch (Throwable ex) {
            log.error(
                "Failed to dead-letter WriteRequest {} (dropping it) - retry state not preserved",
                envelope.getRequest().getRequestId(), ex
            );
        }
    }

}
