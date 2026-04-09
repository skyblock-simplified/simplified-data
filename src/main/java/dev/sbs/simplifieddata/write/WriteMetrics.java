package dev.sbs.simplifieddata.write;

import com.hazelcast.collection.IQueue;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.source.WriteRequest;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import lombok.extern.log4j.Log4j2;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Central owner of every Micrometer meter exposed by the
 * {@code simplified-data} write path. Registered as a Spring {@code @Component}
 * so Spring Boot Actuator's auto-configured {@link MeterRegistry} (provided via
 * the transitive {@code server-api -> spring-boot-starter-actuator} chain) is
 * injected through the constructor, and every other write-path component can
 * get a typed reference by autowiring {@code WriteMetrics} directly.
 *
 * <p>The catalog shipped in Phase 6b.3 covers:
 * <ul>
 *   <li>8 {@code io.micrometer.core.instrument.Counter} instruments:
 *       {@link #METER_REQUESTS_RECEIVED requests.received},
 *       {@link #METER_REQUESTS_RETRIED requests.retried},
 *       {@link #METER_REQUESTS_SKIPPED requests.skipped},
 *       {@link #METER_COMMITS_SUCCESS commits.success},
 *       {@link #METER_COMMITS_FAILURE commits.failure},
 *       {@link #METER_COMMITS_RETRIES_EXHAUSTED commits.retries.exhausted},
 *       {@link #METER_DEADLETTER_ADDED deadletter.added},
 *       {@link #METER_ESCALATIONS escalations}</li>
 *   <li>4 {@code io.micrometer.core.instrument.Timer} instruments:
 *       {@link #METER_COMMITS_DURATION commits.duration},
 *       {@link #METER_GIT_DATA_STEP_DURATION commits.git_data.step_duration},
 *       {@link #METER_DISPATCH_LATENCY dispatch.latency},
 *       {@link #METER_END_TO_END_LATENCY end_to_end.latency}</li>
 *   <li>4 {@code io.micrometer.core.instrument.Gauge} instruments (registered
 *       lazily once by the owning component):
 *       {@link #METER_BUFFER_DEPTH buffer.depth} (one per entity type),
 *       {@link #METER_RETRY_IMAP_SIZE retry_imap.size},
 *       {@link #METER_DEADLETTER_IMAP_SIZE deadletter_imap.size},
 *       {@link #METER_PRIMARY_QUEUE_SIZE primary_queue.size}</li>
 * </ul>
 *
 * <p><b>Cardinality discipline</b>: every tag on every meter is either a bounded
 * enum value ({@link CommitMode}, {@link SkipReason}, {@link FailureReason},
 * {@link GitDataStep}) or an entity class FQCN bounded at 41 (one per SkyBlock
 * entity). No request ids, no timestamps, no free-form strings, no producer
 * ids. Producer ids stay in logs only via
 * {@link WriteRequest#getRequestId()}. Unbounded cardinality tags can blow up a
 * Prometheus time series store so this constraint is non-negotiable.
 *
 * <p><b>Thread safety</b>: all Micrometer instruments are thread-safe. Callers
 * can invoke any {@code record*} method from any thread, and gauges sampled
 * by Micrometer's scheduled harness are similarly thread-safe as long as the
 * supplied {@link Supplier} is itself thread-safe (Hazelcast {@code IMap.size()}
 * and {@code IQueue.size()} are; Java's {@code ConcurrentMap.size()} is).
 *
 * <p><b>Registration pattern</b>: counters and timers are registered eagerly in
 * the constructor so their initial sample (all-zero) appears in the first
 * Prometheus scrape even before any write activity. Gauges are registered
 * lazily by the owning component on its first meaningful lifecycle event
 * (the {@link dev.sbs.simplifieddata.persistence.WritableRemoteJsonSource}
 * constructor for buffer depth; the {@link WriteQueueConsumer#start()} method
 * for the three Hazelcast gauges). This split keeps the Prometheus output
 * complete on day one while avoiding a circular bean dependency between
 * {@code WriteMetrics} and {@code WritableRemoteJsonSource}.
 *
 * @see io.micrometer.core.instrument.Counter
 * @see io.micrometer.core.instrument.Timer
 * @see io.micrometer.core.instrument.Gauge
 */
@Component
@Log4j2
public class WriteMetrics {

    // --- Meter names (stable) --- //

    /** Every fresh WriteRequest drained from the primary IQueue. Rate = producer throughput. */
    public static final @NotNull String METER_REQUESTS_RECEIVED = "skyblock.writes.requests.received";

    /** Every retry entry pulled from the retry IMap. Tagged by attempt counter. */
    public static final @NotNull String METER_REQUESTS_RETRIED = "skyblock.writes.requests.retried";

    /** Dispatch-time skip (unknown class, malformed JSON, missing source). */
    public static final @NotNull String METER_REQUESTS_SKIPPED = "skyblock.writes.requests.skipped";

    /** Every successful commit tick. Tagged by commit mode. */
    public static final @NotNull String METER_COMMITS_SUCCESS = "skyblock.writes.commits.success";

    /** Every failed commit tick. Tagged by commit mode and failure reason. */
    public static final @NotNull String METER_COMMITS_FAILURE = "skyblock.writes.commits.failure";

    /** Count of ticks that exhausted all immediate retries before escalating. */
    public static final @NotNull String METER_COMMITS_RETRIES_EXHAUSTED = "skyblock.writes.commits.retries.exhausted";

    /** Every entry added to the dead-letter IMap. Tagged by entity class FQCN. Phase 6b.4 page alert source. */
    public static final @NotNull String METER_DEADLETTER_ADDED = "skyblock.writes.deadletter.added";

    /** Every mutation escalated from a failed tick to the retry IMap. Tagged by commit mode. */
    public static final @NotNull String METER_ESCALATIONS = "skyblock.writes.escalations";

    /** End-to-end commit timing including all immediate retries. Tagged by mode + status. */
    public static final @NotNull String METER_COMMITS_DURATION = "skyblock.writes.commits.duration";

    /** Per-step timing inside the Git Data API 6-step flow. Tagged by step. */
    public static final @NotNull String METER_GIT_DATA_STEP_DURATION = "skyblock.writes.commits.git_data.step_duration";

    /** Producer-to-consumer-buffer latency (WriteRequest.timestamp to BufferedMutation.bufferedAt). */
    public static final @NotNull String METER_DISPATCH_LATENCY = "skyblock.writes.dispatch.latency";

    /** BufferedMutation to committed-to-GitHub latency. */
    public static final @NotNull String METER_END_TO_END_LATENCY = "skyblock.writes.end_to_end.latency";

    /** Per-type WritableRemoteJsonSource buffer depth. Tagged by entity type. */
    public static final @NotNull String METER_BUFFER_DEPTH = "skyblock.writes.buffer.depth";

    /** Hazelcast retry IMap size. */
    public static final @NotNull String METER_RETRY_IMAP_SIZE = "skyblock.writes.retry_imap.size";

    /** Hazelcast dead-letter IMap size. */
    public static final @NotNull String METER_DEADLETTER_IMAP_SIZE = "skyblock.writes.deadletter_imap.size";

    /** Hazelcast primary IQueue size. */
    public static final @NotNull String METER_PRIMARY_QUEUE_SIZE = "skyblock.writes.primary_queue.size";

    // --- Tag keys --- //

    private static final @NotNull String TAG_ATTEMPT = "attempt";
    private static final @NotNull String TAG_REASON = "reason";
    private static final @NotNull String TAG_MODE = "mode";
    private static final @NotNull String TAG_STATUS = "status";
    private static final @NotNull String TAG_STEP = "step";
    private static final @NotNull String TAG_TYPE = "type";

    private static final @NotNull String STATUS_SUCCESS = "success";
    private static final @NotNull String STATUS_FAILURE = "failure";

    // --- State --- //

    private final @NotNull MeterRegistry registry;

    /**
     * Constructs the metrics holder and eagerly registers every counter in the
     * catalog so their first Prometheus scrape shows the full meter set even
     * when no write activity has occurred. Timers and gauges register lazily
     * on first use.
     *
     * @param meterRegistry the Spring Boot Actuator auto-configured registry
     */
    public WriteMetrics(@NotNull MeterRegistry meterRegistry) {
        this.registry = meterRegistry;

        // Eagerly register every counter with its baseline (zero) tag set so
        // Prometheus scrape 1 shows the full meter catalog. Tagged counters with
        // dynamic tag values cannot be pre-registered per tag combination
        // exhaustively, but the parent meter family is visible as soon as the
        // first sample is recorded.
        this.registry.counter(METER_REQUESTS_RECEIVED);
        this.registry.counter(METER_COMMITS_SUCCESS, TAG_MODE, CommitMode.GIT_DATA.tagValue);
        this.registry.counter(METER_COMMITS_SUCCESS, TAG_MODE, CommitMode.CONTENTS.tagValue);
        this.registry.counter(METER_COMMITS_RETRIES_EXHAUSTED, TAG_MODE, CommitMode.GIT_DATA.tagValue);
        this.registry.counter(METER_COMMITS_RETRIES_EXHAUSTED, TAG_MODE, CommitMode.CONTENTS.tagValue);
        this.registry.counter(METER_ESCALATIONS, TAG_MODE, CommitMode.GIT_DATA.tagValue);
        this.registry.counter(METER_ESCALATIONS, TAG_MODE, CommitMode.CONTENTS.tagValue);

        log.info("WriteMetrics registered {} seed counters in registry {}", 7, meterRegistry.getClass().getSimpleName());
    }

    // --- Counter helpers --- //

    /** Records a fresh write drained from the primary IQueue. */
    public void recordFreshDispatch() {
        this.registry.counter(METER_REQUESTS_RECEIVED).increment();
    }

    /** Records a retry pulled from the retry IMap, tagged by attempt counter (1-based). */
    public void recordRetryDispatch(int attempt) {
        this.registry.counter(METER_REQUESTS_RETRIED, TAG_ATTEMPT, Integer.toString(attempt)).increment();
    }

    /** Records a dispatch-time skip with a bounded-enum reason. */
    public void recordSkipped(@NotNull SkipReason reason) {
        this.registry.counter(METER_REQUESTS_SKIPPED, TAG_REASON, reason.tagValue).increment();
    }

    /** Records an entry added to the dead-letter IMap, tagged by entity class FQCN. */
    public void recordDeadLetter(@NotNull String entityClassName) {
        this.registry.counter(METER_DEADLETTER_ADDED, TAG_TYPE, entityClassName).increment();
    }

    /** Records an escalation of {@code count} mutations to the retry IMap, tagged by commit mode. */
    public void recordEscalation(@NotNull CommitMode mode, int count) {
        if (count > 0)
            this.registry.counter(METER_ESCALATIONS, TAG_MODE, mode.tagValue).increment(count);
    }

    /** Records a retry-budget-exhausted event, tagged by commit mode. */
    public void recordRetriesExhausted(@NotNull CommitMode mode) {
        this.registry.counter(METER_COMMITS_RETRIES_EXHAUSTED, TAG_MODE, mode.tagValue).increment();
    }

    // --- Timer helpers --- //

    /**
     * Starts a commit timer sample. The caller must invoke
     * {@link #recordCommitSuccess(Timer.Sample, CommitMode)} or
     * {@link #recordCommitFailure(Timer.Sample, CommitMode, FailureReason)}
     * exactly once on the returned sample.
     */
    public @NotNull Timer.Sample recordCommitStart() {
        return Timer.start(this.registry);
    }

    /** Stops a commit sample as a success and increments the matching success counter. */
    public void recordCommitSuccess(@NotNull Timer.Sample sample, @NotNull CommitMode mode) {
        sample.stop(this.registry.timer(METER_COMMITS_DURATION, Tags.of(Tag.of(TAG_MODE, mode.tagValue), Tag.of(TAG_STATUS, STATUS_SUCCESS))));
        this.registry.counter(METER_COMMITS_SUCCESS, TAG_MODE, mode.tagValue).increment();
    }

    /** Stops a commit sample as a failure, increments the failure counter tagged by reason. */
    public void recordCommitFailure(@NotNull Timer.Sample sample, @NotNull CommitMode mode, @NotNull FailureReason reason) {
        sample.stop(this.registry.timer(METER_COMMITS_DURATION, Tags.of(Tag.of(TAG_MODE, mode.tagValue), Tag.of(TAG_STATUS, STATUS_FAILURE))));
        this.registry.counter(METER_COMMITS_FAILURE, TAG_MODE, mode.tagValue, TAG_REASON, reason.tagValue).increment();
    }

    /** Starts a Git Data API per-step timer sample. The caller must invoke {@link #recordGitDataStepEnd(Timer.Sample, GitDataStep)} exactly once. */
    public @NotNull Timer.Sample recordGitDataStepStart() {
        return Timer.start(this.registry);
    }

    /** Stops a Git Data API per-step sample, tagged by step. */
    public void recordGitDataStepEnd(@NotNull Timer.Sample sample, @NotNull GitDataStep step) {
        sample.stop(this.registry.timer(METER_GIT_DATA_STEP_DURATION, TAG_STEP, step.tagValue));
    }

    /** Records producer-to-consumer dispatch latency, measured from {@code producerTimestamp} to now. */
    public void recordDispatchLatency(@NotNull Instant producerTimestamp) {
        this.registry.timer(METER_DISPATCH_LATENCY).record(Duration.between(producerTimestamp, Instant.now()));
    }

    /** Records buffered-to-committed latency, measured from {@code bufferedAt} to now. */
    public void recordEndToEndLatency(@NotNull Instant bufferedAt) {
        this.registry.timer(METER_END_TO_END_LATENCY).record(Duration.between(bufferedAt, Instant.now()));
    }

    // --- Gauge registration (lazy, one-shot) --- //

    /**
     * Registers a per-type buffer depth gauge for a
     * {@link dev.sbs.simplifieddata.persistence.WritableRemoteJsonSource}
     * instance. Invoked once per source by the source's constructor, so each
     * of the 41 SkyBlock entity types ends up with exactly one tagged gauge
     * instance in the registry.
     *
     * @param entityType the entity class - used to tag the gauge
     * @param sizeSupplier a thread-safe supplier of the current buffer size
     */
    public void registerBufferDepthGauge(@NotNull Class<? extends JpaModel> entityType, @NotNull Supplier<Number> sizeSupplier) {
        this.registry.gauge(
            METER_BUFFER_DEPTH,
            Tags.of(TAG_TYPE, entityType.getName()),
            sizeSupplier,
            supplier -> supplier.get().doubleValue()
        );
    }

    /** Registers the Hazelcast retry IMap size gauge. Called once by {@link WriteQueueConsumer#start()}. */
    public void registerRetryImapSizeGauge(@NotNull HazelcastInstance instance) {
        IMap<UUID, RetryEnvelope> retryMap = instance.getMap(WriteQueueConsumer.RETRY_MAP_NAME);
        this.registry.gauge(METER_RETRY_IMAP_SIZE, retryMap, IMap::size);
    }

    /** Registers the Hazelcast dead-letter IMap size gauge. Called once by {@link WriteQueueConsumer#start()}. */
    public void registerDeadletterImapSizeGauge(@NotNull HazelcastInstance instance) {
        IMap<UUID, WriteRequest> deadletter = instance.getMap(WriteQueueConsumer.DEADLETTER_MAP_NAME);
        this.registry.gauge(METER_DEADLETTER_IMAP_SIZE, deadletter, IMap::size);
    }

    /** Registers the Hazelcast primary IQueue size gauge. Called once by {@link WriteQueueConsumer#start()}. */
    public void registerPrimaryQueueSizeGauge(@NotNull HazelcastInstance instance) {
        IQueue<WriteRequest> queue = instance.getQueue(WriteQueueConsumer.QUEUE_NAME);
        this.registry.gauge(METER_PRIMARY_QUEUE_SIZE, queue, IQueue::size);
    }

    /** Exposes the underlying registry for tests and Spring Boot Actuator introspection. */
    public @NotNull MeterRegistry getRegistry() {
        return this.registry;
    }

    // --- Bounded-enum tag values --- //

    /**
     * Commit mode discriminator tag. Mirrors {@link WriteMode} but lives here
     * so tag values are defined in one place and the cardinality constraint is
     * obvious from this class alone.
     */
    public enum CommitMode {

        GIT_DATA("git_data"),
        CONTENTS("contents");

        public final @NotNull String tagValue;

        CommitMode(@NotNull String tagValue) {
            this.tagValue = tagValue;
        }

        /** Maps a {@link WriteMode} to the matching commit-mode tag. */
        public static @NotNull CommitMode from(@NotNull WriteMode mode) {
            return switch (mode) {
                case GIT_DATA -> GIT_DATA;
                case CONTENTS -> CONTENTS;
            };
        }

    }

    /**
     * Dispatch-time skip reason. Bounded at 3 values, each corresponding to a
     * specific {@code WriteQueueConsumer.dispatch} skip branch.
     */
    public enum SkipReason {

        UNKNOWN_TYPE("unknown_type"),
        MALFORMED_JSON("malformed_json"),
        MISSING_SOURCE("missing_source");

        public final @NotNull String tagValue;

        SkipReason(@NotNull String tagValue) {
            this.tagValue = tagValue;
        }

    }

    /**
     * Commit failure reason. Bounded at 6 values covering the GitHub API's
     * known failure modes for the write path.
     */
    public enum FailureReason {

        STATUS_422("422_non_ff"),
        STATUS_409("409_conflict"),
        STATUS_5XX("5xx"),
        NETWORK("network"),
        AUTH("auth"),
        OTHER("other");

        public final @NotNull String tagValue;

        FailureReason(@NotNull String tagValue) {
            this.tagValue = tagValue;
        }

        /** Maps an HTTP status code to a matching failure reason. */
        public static @NotNull FailureReason fromStatus(int status) {
            if (status == 422)
                return STATUS_422;
            if (status == 409)
                return STATUS_409;
            if (status >= 500 && status < 600)
                return STATUS_5XX;
            if (status == 401 || status == 403)
                return AUTH;
            return OTHER;
        }

    }

    /**
     * Git Data API step discriminator. Bounded at 6 values, one per step in
     * the {@link GitDataCommitService#commit} flow.
     */
    public enum GitDataStep {

        GET_REF("get_ref"),
        GET_COMMIT("get_commit"),
        CREATE_BLOB("create_blob"),
        CREATE_TREE("create_tree"),
        CREATE_COMMIT("create_commit"),
        UPDATE_REF("update_ref");

        public final @NotNull String tagValue;

        GitDataStep(@NotNull String tagValue) {
            this.tagValue = tagValue;
        }

    }

}
