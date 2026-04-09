package dev.sbs.simplifieddata.write;

import com.hazelcast.collection.IQueue;
import com.hazelcast.core.HazelcastInstance;
import dev.sbs.minecraftapi.MinecraftApi;
import dev.sbs.minecraftapi.persistence.model.ZodiacEvent;
import dev.simplified.persistence.source.WriteRequest;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.log4j.Log4j2;
import org.jetbrains.annotations.NotNull;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Phase 6b gate-7 smoke harness bean. Only active when the Spring profile
 * {@code smoke} is set via {@code SPRING_PROFILES_ACTIVE=smoke}.
 *
 * <p>On context refresh this bean emits a two-phase self-cleaning smoke run:
 * <ol>
 *   <li><b>Phase 1 ({@code @PostConstruct}):</b> puts an UPSERT
 *       {@link WriteRequest} for a sentinel {@link ZodiacEvent} with fixed
 *       id {@link #SENTINEL_ID} onto the Hazelcast
 *       {@link WriteQueueConsumer#QUEUE_NAME} queue. The
 *       {@link WriteBatchScheduler}'s next tick (default 10s) stages and
 *       commits the mutation to {@code skyblock-data} master.</li>
 *   <li><b>Phase 2 ({@link #CLEANUP_DELAY} later):</b> a daemon
 *       {@link ScheduledExecutorService} fires once and emits a DELETE
 *       {@link WriteRequest} for the same sentinel id. The scheduler's next
 *       tick commits the removal to master, returning the file to its
 *       pre-run state.</li>
 * </ol>
 *
 * <p>Both phases produce distinct commits on the
 * {@code skyblock-data} master branch and both exercise the full write
 * path (Hazelcast IQueue → WriteQueueConsumer → WritableRemoteJsonSource
 * buffer → WriteBatchScheduler → GitDataCommitService → GitHub). Phase 1
 * proves upsert + commit creation; phase 2 proves delete + file cleanup.
 * The Phase 5.5 {@code AssetPoller} detects both commits within one poll
 * cycle ({@code ~60s} each), firing a targeted refresh on the
 * {@link ZodiacEvent} model class both times. The profile should NEVER be
 * active in production.
 *
 * <p>The cleanup delay ({@link #CLEANUP_DELAY}) must be long enough for
 * phase 1's upsert to land on master before the delete is enqueued.
 * Otherwise the delete would land in the same {@code WritableRemoteJsonSource}
 * buffer as the upsert, overwrite it via the
 * {@code ConcurrentMap<String, BufferedMutation>} per-id replace semantics,
 * and collapse the two-operation run into just a delete (which is a silent
 * no-op if the sentinel was not on master before the run). 30 seconds
 * covers the typical flow: WriteBatchScheduler 10s tick + ~3s commit
 * latency + safety margin. Under sustained write failures the retry path
 * may extend the upsert's effective commit time beyond 30 seconds; in that
 * edge case the delete collapses with the pending retry and operators
 * should manually revert from master.
 *
 * <p>Container teardown within the cleanup window cancels the pending
 * delete task (daemon thread dies with the JVM), leaving the sentinel on
 * master until the operator reverts it manually. This is accepted - the
 * smoke harness is opt-in and operators who kill the container during a
 * run accept the cleanup burden.
 *
 * <p>Idempotency: the sentinel's {@link ZodiacEvent#getName() name} field
 * embeds a per-boot {@link Instant#now()} timestamp so the serialized JSON
 * body differs from every previous run's body. This sidesteps the Phase 6c
 * gate-7 debugging finding: if a previous smoke run was never reverted from
 * {@code skyblock-data} master and this harness emitted byte-identical
 * sentinel state, {@link dev.sbs.simplifieddata.persistence.WritableRemoteJsonSource#stageBatch()}
 * would correctly detect the no-op and suppress the upsert commit, making
 * the gate appear stuck. With a timestamp-varying name field every run
 * produces a distinguishable upsert commit even when stacking against a
 * non-reverted prior run.
 */
@Component
@Profile("smoke")
@Log4j2
public class SmokeWriteSentinel {

    /** Sentinel entity id - chosen to be unmistakably a test artifact. */
    public static final @NotNull String SENTINEL_ID = "SBS_WRITE_SMOKE_TEST";

    /** Sentinel entity {@code releaseYear} - fixed value, not used for idempotency. */
    static final int SENTINEL_RELEASE_YEAR = 999;

    /**
     * Prefix of the sentinel entity {@code name}. The full name is this prefix
     * followed by {@code " @ "} and the {@link Instant#now()} timestamp at boot,
     * producing a distinct serialized form on every run.
     */
    static final @NotNull String SENTINEL_NAME_PREFIX = "Phase 6b Smoke Test";

    /**
     * Delay between the phase 1 upsert enqueue and the phase 2 delete
     * enqueue. Must exceed {@code WriteBatchScheduler}'s tick interval
     * (default 10s) plus commit latency so the upsert has a chance to reach
     * GitHub before the delete lands in the source buffer.
     */
    static final @NotNull Duration CLEANUP_DELAY = Duration.ofSeconds(30);

    private final @NotNull HazelcastInstance writeHazelcastInstance;

    /**
     * Single-threaded daemon executor used to schedule the phase 2 cleanup
     * delete. Daemon so it does not block JVM shutdown; single-threaded
     * because the harness only schedules one task per bean lifetime. Shut
     * down via {@link #shutdownCleanupScheduler()} on bean teardown.
     */
    private final @NotNull ScheduledExecutorService cleanupScheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "smoke-sentinel-cleanup");
        thread.setDaemon(true);
        return thread;
    });

    public SmokeWriteSentinel(@NotNull HazelcastInstance skyBlockWriteHazelcastInstance) {
        this.writeHazelcastInstance = skyBlockWriteHazelcastInstance;
    }

    /**
     * Phase 1: emits the UPSERT sentinel WriteRequest at bean init and
     * schedules the phase 2 cleanup delete for {@link #CLEANUP_DELAY} later.
     */
    @PostConstruct
    void emitSentinelOnBoot() {
        try {
            String sentinelName = SENTINEL_NAME_PREFIX + " @ " + Instant.now();
            WriteRequest upsert = this.buildUpsertRequest(sentinelName);
            this.enqueue(upsert);

            log.warn(
                "SMOKE PROFILE ACTIVE: emitted UPSERT sentinel WriteRequest {} (ZodiacEvent id='{}' name='{}') - "
                    + "expected path: WriteQueueConsumer drains within ~1s, WriteBatchScheduler commits within ~10s. "
                    + "Phase 2 cleanup DELETE scheduled for {} from now.",
                upsert.getRequestId(), SENTINEL_ID, sentinelName, CLEANUP_DELAY
            );

            this.cleanupScheduler.schedule(
                this::emitSentinelCleanup,
                CLEANUP_DELAY.toMillis(),
                TimeUnit.MILLISECONDS
            );
        } catch (Throwable ex) {
            log.error("SMOKE PROFILE ACTIVE: failed to emit phase 1 UPSERT sentinel WriteRequest", ex);
        }
    }

    /**
     * Phase 2: emits the DELETE sentinel WriteRequest after the cleanup
     * delay elapses. Fired from the daemon {@link #cleanupScheduler} thread,
     * so exceptions are swallowed here to prevent silent executor death -
     * the operator still has the manual-revert escape hatch if this fails.
     */
    private void emitSentinelCleanup() {
        try {
            WriteRequest delete = this.buildDeleteRequest();
            this.enqueue(delete);

            log.warn(
                "SMOKE PROFILE ACTIVE: emitted DELETE cleanup WriteRequest {} (ZodiacEvent id='{}') - "
                    + "expected path: WriteBatchScheduler commits within ~10s, returning skyblock-data master to pre-run state.",
                delete.getRequestId(), SENTINEL_ID
            );
        } catch (Throwable ex) {
            log.error(
                "SMOKE PROFILE ACTIVE: failed to emit phase 2 DELETE cleanup WriteRequest - "
                    + "the sentinel is still on skyblock-data master and must be reverted manually",
                ex
            );
        }
    }

    /**
     * Shuts down the cleanup scheduler on bean teardown. Used
     * {@link ScheduledExecutorService#shutdownNow()} to interrupt any
     * in-flight cleanup task, matching the accepted contract that container
     * teardown mid-run leaves cleanup responsibility with the operator.
     */
    @PreDestroy
    void shutdownCleanupScheduler() {
        this.cleanupScheduler.shutdownNow();
    }

    private @NotNull WriteRequest buildUpsertRequest(@NotNull String sentinelName) {
        ZodiacEvent sentinel = new ZodiacEvent();
        setField(sentinel, "id", SENTINEL_ID);
        setField(sentinel, "name", sentinelName);
        setField(sentinel, "releaseYear", SENTINEL_RELEASE_YEAR);
        return WriteRequest.upsert(ZodiacEvent.class, sentinel, MinecraftApi.getGson(), "skyblock-data");
    }

    private @NotNull WriteRequest buildDeleteRequest() {
        // Name and releaseYear are irrelevant for DELETE dispatch (the
        // source matches on id only), but set them for audit log readability
        // when the operator inspects the dead-letter IMap.
        ZodiacEvent sentinel = new ZodiacEvent();
        setField(sentinel, "id", SENTINEL_ID);
        setField(sentinel, "name", SENTINEL_NAME_PREFIX + " (cleanup)");
        setField(sentinel, "releaseYear", SENTINEL_RELEASE_YEAR);
        return WriteRequest.delete(ZodiacEvent.class, sentinel, MinecraftApi.getGson(), "skyblock-data");
    }

    private void enqueue(@NotNull WriteRequest request) throws InterruptedException {
        IQueue<WriteRequest> queue = this.writeHazelcastInstance.getQueue(WriteQueueConsumer.QUEUE_NAME);
        queue.put(request);
    }

    private static void setField(@NotNull Object target, @NotNull String fieldName, @NotNull Object value) {
        try {
            Field field = ZodiacEvent.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(target, value);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

}
