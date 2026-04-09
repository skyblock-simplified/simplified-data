package dev.sbs.simplifieddata.write;

import com.google.gson.Gson;
import com.hazelcast.collection.IQueue;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import dev.sbs.minecraftapi.MinecraftApi;
import dev.sbs.simplifieddata.persistence.RemoteSkyBlockFactory;
import dev.sbs.simplifieddata.persistence.WritableRemoteJsonSource;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.source.WriteRequest;
import jakarta.annotation.PreDestroy;
import lombok.extern.log4j.Log4j2;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;

/**
 * Drains the {@code skyblock.writes} Hazelcast {@link IQueue} + a local
 * {@link DelayQueue} of retry-pending requests on a dedicated daemon thread,
 * dispatching each drained {@link WriteRequest} into the per-entity
 * {@link WritableRemoteJsonSource} buffer owned by
 * {@link RemoteSkyBlockFactory#getWritableSources()}.
 *
 * <p>Started by an {@code @EventListener(ApplicationReadyEvent)} handler so
 * the drain begins once Spring finishes refreshing the context (same pattern
 * as {@link dev.sbs.simplifieddata.poller.AssetPoller}). Shutdown is managed
 * by {@code @PreDestroy} which interrupts the daemon thread and waits briefly
 * for clean exit.
 *
 * <p>Drain loop: each iteration polls the Hazelcast queue and the local
 * {@link DelayQueue} alternately. When a request is available, the consumer:
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
 *   <li>Invokes the source's {@code upsert}/{@code delete} via an unchecked
 *       cast to {@code WritableRemoteJsonSource<JpaModel>} - justified because
 *       the entity type came from the same {@link WriteRequest} envelope that
 *       pinned the source type in {@code getWritableSources()}.</li>
 *   <li>Catches any {@link Throwable} from the body, logs ERROR with the
 *       full {@link WriteRequest#getRequestId()} for traceability, and
 *       continues the loop so a single bad request cannot stop the drain.</li>
 * </ol>
 *
 * <p>Exponential-backoff retry path: failed requests are pushed onto the
 * local {@link DelayQueue} with increasing delays (1m, 2m, 4m, 8m, 16m by
 * default), up to {@code maxRetryAttempts}. After the final attempt fails,
 * the request is dead-lettered to
 * {@code IMap<UUID, WriteRequest> skyblock.writes.deadletter} for operator
 * inspection. The dead-letter map is auto-created on first access.
 *
 * <p>Restart semantics: the local {@link DelayQueue} is in-memory only, so
 * requests currently in a backoff window are lost on consumer restart. This
 * is an accepted Phase 6b trade-off; a Phase 6e follow-up could move the
 * retry state to a Hazelcast IMap with an {@code @EventListener} bootstrap
 * so in-flight retries survive restart.
 *
 * <p>The consumer does NOT call {@code commitBatch} itself - the
 * {@link WriteBatchScheduler} owns that responsibility on its 10s cadence.
 * Failed commits that come back from the scheduler are surfaced to this
 * consumer via {@link #scheduleRetry(DelayedWriteRequest)}.
 *
 * @see WriteBatchScheduler
 * @see WritableRemoteJsonSource
 * @see DelayedWriteRequest
 */
@Component
@Log4j2
public class WriteQueueConsumer {

    /** The Hazelcast IQueue name declared in {@code infra/hazelcast/hazelcast.xml}. */
    public static final @NotNull String QUEUE_NAME = "skyblock.writes";

    /** The Hazelcast IMap name used for the dead-letter dump. Auto-created on first access. */
    public static final @NotNull String DEADLETTER_MAP_NAME = "skyblock.writes.deadletter";

    private final @NotNull HazelcastInstance writeHazelcastInstance;
    private final @NotNull RemoteSkyBlockFactory factory;
    private final @NotNull Gson gson;
    private final @NotNull Duration retryInitialDelay;
    private final int maxRetryAttempts;
    private final boolean enabled;

    /**
     * In-memory exponential-backoff retry queue. Survives the lifetime of
     * the consumer only; restart clears it.
     */
    private final @NotNull DelayQueue<DelayedWriteRequest> retryQueue = new DelayQueue<>();

    private volatile Thread drainThread;
    private volatile boolean stopping = false;

    public WriteQueueConsumer(
        @NotNull HazelcastInstance skyBlockWriteHazelcastInstance,
        @NotNull RemoteSkyBlockFactory remoteSkyBlockFactory,
        @Value("${skyblock.data.github.write-retry-initial-delay-minutes:1}") long retryInitialDelayMinutes,
        @Value("${skyblock.data.github.write-retry-max-attempts:5}") int maxRetryAttempts,
        @Value("${skyblock.data.github.write-consumer-enabled:true}") boolean enabled
    ) {
        this.writeHazelcastInstance = skyBlockWriteHazelcastInstance;
        this.factory = remoteSkyBlockFactory;
        this.gson = MinecraftApi.getGson();
        this.retryInitialDelay = Duration.ofMinutes(retryInitialDelayMinutes);
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

        Thread thread = new Thread(this::drainLoop, "skyblock-write-consumer");
        thread.setDaemon(true);
        this.drainThread = thread;
        thread.start();
        log.info("WriteQueueConsumer daemon thread started (queue='{}')", QUEUE_NAME);
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
     * Pushes a failed write back onto the retry queue with the supplied
     * {@link DelayedWriteRequest}. Called by {@link WriteBatchScheduler}
     * for each failed mutation returned by a per-source {@code commitBatch}
     * tick.
     *
     * @param delayed the retry entry, carrying the hydrated mutation and ready-at instant
     */
    public void scheduleRetry(@NotNull DelayedWriteRequest delayed) {
        if (delayed.getAttempt() > this.maxRetryAttempts) {
            this.deadLetter(delayed);
            return;
        }

        this.retryQueue.offer(delayed);
        log.info(
            "scheduled retry for WriteRequest {} attempt={} readyAt={}",
            delayed.getMutation().getRequestId(), delayed.getAttempt(), delayed.getReadyAt()
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
                // 1. Prefer fresh writes from the primary queue - short poll so delayed retries don't starve.
                WriteRequest fresh = queue.poll(500, TimeUnit.MILLISECONDS);
                if (fresh != null) {
                    this.dispatchFresh(fresh);
                    continue;
                }

                // 2. Drain any ready retries from the local DelayQueue.
                DelayedWriteRequest delayed = this.retryQueue.poll();
                if (delayed != null) {
                    this.dispatchRetry(delayed);
                }
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
     * Dispatches a fresh {@link WriteRequest} from the Hazelcast IQueue.
     * Resolves the entity class, deserializes the JSON, and buffers the
     * resulting {@link BufferedMutation} into the matching source. Any
     * failure is logged and the request is skipped - dispatch-time failures
     * typically indicate schema drift (unknown class, non-JpaModel target,
     * malformed JSON) which retrying would not fix.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void dispatchFresh(@NotNull WriteRequest request) {
        try {
            Class<? extends JpaModel> type = request.resolveEntityType();
            WritableRemoteJsonSource source = this.factory.getWritableSources().get(type);

            if (source == null) {
                log.warn(
                    "No writable source registered for type '{}' - skipping WriteRequest {}",
                    type.getName(), request.getRequestId()
                );
                return;
            }

            JpaModel entity = request.deserializeEntity(this.gson, type);
            BufferedMutation<JpaModel> mutation = new BufferedMutation<>(
                request.getOperation(), entity, request.getRequestId(), Instant.now()
            );
            source.buffer(mutation);

            log.info(
                "consumed WriteRequest {} op={} type={}",
                request.getRequestId(), request.getOperation(), type.getSimpleName()
            );
        } catch (JpaException ex) {
            log.warn(
                "Skipping WriteRequest {} - unable to resolve or buffer: {}",
                request.getRequestId(), ex.getMessage()
            );
        } catch (Throwable ex) {
            log.error("Unexpected error dispatching WriteRequest {}", request.getRequestId(), ex);
        }
    }

    /**
     * Re-buffers a {@link DelayedWriteRequest} into its target source. The
     * mutation is already hydrated and the target class is already known,
     * so this path skips the deserialize step entirely.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void dispatchRetry(@NotNull DelayedWriteRequest delayed) {
        try {
            WritableRemoteJsonSource source = this.factory.getWritableSources().get(delayed.getEntityType());

            if (source == null) {
                log.warn(
                    "No writable source for retry of type '{}' - dropping WriteRequest {}",
                    delayed.getEntityType().getName(), delayed.getMutation().getRequestId()
                );
                return;
            }

            source.buffer(delayed.getMutation());

            log.info(
                "re-buffered retry WriteRequest {} op={} type={} attempt={}",
                delayed.getMutation().getRequestId(),
                delayed.getMutation().getOperation(),
                delayed.getEntityType().getSimpleName(),
                delayed.getAttempt()
            );
        } catch (JpaException ex) {
            log.warn(
                "Skipping retry WriteRequest {} - buffer threw: {}",
                delayed.getMutation().getRequestId(), ex.getMessage()
            );
        } catch (Throwable ex) {
            log.error(
                "Unexpected error re-buffering retry WriteRequest {}",
                delayed.getMutation().getRequestId(), ex
            );
        }
    }

    /** Pushes a fully-exhausted retry onto the dead-letter IMap for operator inspection. */
    private void deadLetter(@NotNull DelayedWriteRequest delayed) {
        try {
            // Rebuild the WriteRequest envelope for the dead-letter map so operators can inspect
            // the full producer-side payload without reaching into the in-memory mutation.
            @SuppressWarnings({"unchecked", "rawtypes"})
            WriteRequest envelope = switch (delayed.getMutation().getOperation()) {
                case UPSERT -> WriteRequest.upsert(
                    (Class) delayed.getEntityType(),
                    (JpaModel) delayed.getMutation().getEntity(),
                    this.gson,
                    dev.sbs.simplifieddata.config.GitHubConfig.SOURCE_ID
                );
                case DELETE -> WriteRequest.delete(
                    (Class) delayed.getEntityType(),
                    (JpaModel) delayed.getMutation().getEntity(),
                    this.gson,
                    dev.sbs.simplifieddata.config.GitHubConfig.SOURCE_ID
                );
            };
            // Key on the original request id so operator queries against the dead-letter map
            // line up with audit logs from the initial producer put.
            UUID key = delayed.getMutation().getRequestId();
            IMap<UUID, WriteRequest> deadletter = this.writeHazelcastInstance.getMap(DEADLETTER_MAP_NAME);
            deadletter.put(key, envelope);
            log.error(
                "DEAD-LETTER WriteRequest {} op={} type={} after {} attempts - requires operator attention",
                key,
                delayed.getMutation().getOperation(),
                delayed.getEntityType().getName(),
                delayed.getAttempt() - 1
            );
        } catch (Throwable ex) {
            log.error(
                "Failed to dead-letter WriteRequest {} (dropping it) - DelayQueue state lost on restart",
                delayed.getMutation().getRequestId(), ex
            );
        }
    }

}
