package dev.sbs.simplifieddata.write;

import com.hazelcast.collection.IQueue;
import com.hazelcast.core.HazelcastInstance;
import dev.sbs.minecraftapi.MinecraftApi;
import dev.sbs.minecraftapi.persistence.model.ZodiacEvent;
import dev.simplified.persistence.source.WriteRequest;
import jakarta.annotation.PostConstruct;
import lombok.extern.log4j.Log4j2;
import org.jetbrains.annotations.NotNull;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.time.Instant;

/**
 * Phase 6b gate-7 smoke harness bean. Only active when the Spring profile
 * {@code smoke} is set via {@code SPRING_PROFILES_ACTIVE=smoke}.
 *
 * <p>On context refresh this bean puts exactly one synthetic
 * {@link WriteRequest} onto the Hazelcast {@code skyblock.writes} queue -
 * an upsert of a sentinel {@link ZodiacEvent} with fixed id
 * {@link #SENTINEL_ID}. The operator runs the stack, waits for the
 * {@link WriteBatchScheduler}'s next tick (at most
 * {@code skyblock.data.github.write-batch-interval-seconds}, default 10s),
 * observes the GitHub commit on the {@code skyblock-data} {@code master}
 * branch, and then reverts the synthetic mutation by editing the file
 * through a local overlay + restart. The profile should NEVER be active in
 * production.
 *
 * <p>The sentinel id is intentionally outside the existing
 * {@code YEAR_OF_THE_*} namespace so accidental non-revert runs do not
 * clobber real data; the Phase 5.5 {@code AssetPoller}'s next cycle detects
 * the new commit and fires a targeted refresh for the {@link ZodiacEvent}
 * model class, proving the full write-to-read loop end-to-end.
 *
 * <p>Idempotency: the sentinel's {@link ZodiacEvent#getName() name} field
 * embeds a per-boot {@link Instant#now()} timestamp so the serialized JSON
 * body differs from every previous run's body. This sidesteps the Phase 6c
 * gate-7 debugging finding: if a previous smoke run was never reverted from
 * {@code skyblock-data} master and this harness emitted byte-identical
 * sentinel state, {@link dev.sbs.simplifieddata.persistence.WritableRemoteJsonSource#stageBatch()}
 * would correctly detect the no-op and suppress the commit, making the gate
 * appear stuck. With a timestamp-varying name field every run produces a
 * distinguishable commit on {@code master}, keeps the fixed id for easy
 * grep / manual cleanup, and updates the single sentinel row in place
 * instead of accumulating duplicate entries across runs.
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

    private final @NotNull HazelcastInstance writeHazelcastInstance;

    public SmokeWriteSentinel(@NotNull HazelcastInstance skyBlockWriteHazelcastInstance) {
        this.writeHazelcastInstance = skyBlockWriteHazelcastInstance;
    }

    @PostConstruct
    void emitSentinelOnBoot() {
        try {
            String sentinelName = SENTINEL_NAME_PREFIX + " @ " + Instant.now();
            ZodiacEvent sentinel = new ZodiacEvent();
            setField(sentinel, "id", SENTINEL_ID);
            setField(sentinel, "name", sentinelName);
            setField(sentinel, "releaseYear", SENTINEL_RELEASE_YEAR);

            WriteRequest request = WriteRequest.upsert(
                ZodiacEvent.class,
                sentinel,
                MinecraftApi.getGson(),
                "skyblock-data"
            );

            IQueue<WriteRequest> queue = this.writeHazelcastInstance.getQueue(WriteQueueConsumer.QUEUE_NAME);
            queue.put(request);

            log.warn(
                "SMOKE PROFILE ACTIVE: emitted sentinel WriteRequest {} (UPSERT ZodiacEvent id='{}' name='{}') - "
                    + "expected path: WriteQueueConsumer drains within ~1s, WriteBatchScheduler commits within ~10s, "
                    + "AssetPoller detects new commit within ~60s. Revert the sentinel from skyblock-data master after verification.",
                request.getRequestId(), SENTINEL_ID, sentinelName
            );
        } catch (Throwable ex) {
            log.error("SMOKE PROFILE ACTIVE: failed to emit sentinel WriteRequest", ex);
        }
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
