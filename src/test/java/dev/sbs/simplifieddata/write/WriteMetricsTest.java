package dev.sbs.simplifieddata.write;

import com.hazelcast.collection.IQueue;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.source.WriteRequest;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.Serial;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for {@link WriteMetrics}. Uses Micrometer's in-memory
 * {@link SimpleMeterRegistry} (part of {@code micrometer-core}, already on the
 * classpath transitively via {@code spring-boot-starter-actuator}) as the
 * backing registry. Every helper method on {@code WriteMetrics} is exercised
 * once to assert that the expected meter is present with the expected name,
 * tag set, and sample values.
 *
 * <p>The Hazelcast gauges are tested via Mockito-backed
 * {@link HazelcastInstance} / {@link IMap} / {@link IQueue} stubs so the test
 * does not need a live cluster.
 */
class WriteMetricsTest {

    private SimpleMeterRegistry registry;
    private WriteMetrics metrics;

    @BeforeEach
    void setUp() {
        this.registry = new SimpleMeterRegistry();
        this.metrics = new WriteMetrics(this.registry);
    }

    @Test
    @DisplayName("constructor eagerly seeds the counter catalog")
    void constructorSeedsCounterCatalog() {
        // Seed counters: requests.received + commits.success{mode=git_data,contents}
        // + commits.retries.exhausted{mode=git_data,contents} + escalations{mode=git_data,contents}
        assertThat(this.registry.find(WriteMetrics.METER_REQUESTS_RECEIVED).counter(), is(notNullValue()));
        assertThat(this.registry.find(WriteMetrics.METER_COMMITS_SUCCESS).tag("mode", "git_data").counter(), is(notNullValue()));
        assertThat(this.registry.find(WriteMetrics.METER_COMMITS_SUCCESS).tag("mode", "contents").counter(), is(notNullValue()));
        assertThat(this.registry.find(WriteMetrics.METER_COMMITS_RETRIES_EXHAUSTED).tag("mode", "git_data").counter(), is(notNullValue()));
        assertThat(this.registry.find(WriteMetrics.METER_ESCALATIONS).tag("mode", "contents").counter(), is(notNullValue()));
    }

    @Test
    @DisplayName("recordFreshDispatch increments the received counter")
    void recordFreshDispatchIncrementsReceived() {
        this.metrics.recordFreshDispatch();
        this.metrics.recordFreshDispatch();
        this.metrics.recordFreshDispatch();

        Counter counter = this.registry.find(WriteMetrics.METER_REQUESTS_RECEIVED).counter();
        assertThat(counter, is(notNullValue()));
        assertThat(counter.count(), is(equalTo(3.0)));
    }

    @Test
    @DisplayName("recordRetryDispatch tags the counter by attempt number")
    void recordRetryDispatchTagsByAttempt() {
        this.metrics.recordRetryDispatch(1);
        this.metrics.recordRetryDispatch(1);
        this.metrics.recordRetryDispatch(2);
        this.metrics.recordRetryDispatch(5);

        assertThat(this.registry.find(WriteMetrics.METER_REQUESTS_RETRIED).tag("attempt", "1").counter().count(), is(equalTo(2.0)));
        assertThat(this.registry.find(WriteMetrics.METER_REQUESTS_RETRIED).tag("attempt", "2").counter().count(), is(equalTo(1.0)));
        assertThat(this.registry.find(WriteMetrics.METER_REQUESTS_RETRIED).tag("attempt", "5").counter().count(), is(equalTo(1.0)));
    }

    @Test
    @DisplayName("recordSkipped tags the counter by enum reason")
    void recordSkippedTagsByReason() {
        this.metrics.recordSkipped(WriteMetrics.SkipReason.UNKNOWN_TYPE);
        this.metrics.recordSkipped(WriteMetrics.SkipReason.MALFORMED_JSON);
        this.metrics.recordSkipped(WriteMetrics.SkipReason.MISSING_SOURCE);
        this.metrics.recordSkipped(WriteMetrics.SkipReason.MISSING_SOURCE);

        assertThat(this.registry.find(WriteMetrics.METER_REQUESTS_SKIPPED).tag("reason", "unknown_type").counter().count(), is(equalTo(1.0)));
        assertThat(this.registry.find(WriteMetrics.METER_REQUESTS_SKIPPED).tag("reason", "malformed_json").counter().count(), is(equalTo(1.0)));
        assertThat(this.registry.find(WriteMetrics.METER_REQUESTS_SKIPPED).tag("reason", "missing_source").counter().count(), is(equalTo(2.0)));
    }

    @Test
    @DisplayName("recordDeadLetter tags the counter by entity class FQCN")
    void recordDeadLetterTagsByFqcn() {
        this.metrics.recordDeadLetter("dev.sbs.minecraftapi.persistence.model.ZodiacEvent");
        this.metrics.recordDeadLetter("dev.sbs.minecraftapi.persistence.model.Item");

        assertThat(
            this.registry.find(WriteMetrics.METER_DEADLETTER_ADDED).tag("type", "dev.sbs.minecraftapi.persistence.model.ZodiacEvent").counter().count(),
            is(equalTo(1.0))
        );
        assertThat(
            this.registry.find(WriteMetrics.METER_DEADLETTER_ADDED).tag("type", "dev.sbs.minecraftapi.persistence.model.Item").counter().count(),
            is(equalTo(1.0))
        );
    }

    @Test
    @DisplayName("recordEscalation increments by count and tags by mode")
    void recordEscalationIncrementsByCount() {
        this.metrics.recordEscalation(WriteMetrics.CommitMode.GIT_DATA, 5);
        this.metrics.recordEscalation(WriteMetrics.CommitMode.GIT_DATA, 3);
        this.metrics.recordEscalation(WriteMetrics.CommitMode.CONTENTS, 1);
        this.metrics.recordEscalation(WriteMetrics.CommitMode.GIT_DATA, 0); // no-op

        assertThat(this.registry.find(WriteMetrics.METER_ESCALATIONS).tag("mode", "git_data").counter().count(), is(equalTo(8.0)));
        assertThat(this.registry.find(WriteMetrics.METER_ESCALATIONS).tag("mode", "contents").counter().count(), is(equalTo(1.0)));
    }

    @Test
    @DisplayName("recordRetriesExhausted increments the exhausted counter tagged by mode")
    void recordRetriesExhausted() {
        this.metrics.recordRetriesExhausted(WriteMetrics.CommitMode.GIT_DATA);
        this.metrics.recordRetriesExhausted(WriteMetrics.CommitMode.GIT_DATA);
        this.metrics.recordRetriesExhausted(WriteMetrics.CommitMode.CONTENTS);

        assertThat(this.registry.find(WriteMetrics.METER_COMMITS_RETRIES_EXHAUSTED).tag("mode", "git_data").counter().count(), is(equalTo(2.0)));
        assertThat(this.registry.find(WriteMetrics.METER_COMMITS_RETRIES_EXHAUSTED).tag("mode", "contents").counter().count(), is(equalTo(1.0)));
    }

    @Test
    @DisplayName("commit sample records a success duration and increments the success counter")
    void commitSampleRecordsSuccess() {
        Timer.Sample sample = this.metrics.recordCommitStart();
        sleepQuietly(5);
        this.metrics.recordCommitSuccess(sample, WriteMetrics.CommitMode.GIT_DATA);

        Timer timer = this.registry.find(WriteMetrics.METER_COMMITS_DURATION)
            .tags("mode", "git_data", "status", "success")
            .timer();
        assertThat(timer, is(notNullValue()));
        assertThat(timer.count(), is(equalTo(1L)));
        assertThat(timer.totalTime(TimeUnit.MILLISECONDS), is(greaterThanOrEqualTo(1.0)));

        Counter successCounter = this.registry.find(WriteMetrics.METER_COMMITS_SUCCESS).tag("mode", "git_data").counter();
        assertThat(successCounter.count(), is(equalTo(1.0)));
    }

    @Test
    @DisplayName("commit sample records a failure duration and increments the failure counter tagged by reason")
    void commitSampleRecordsFailure() {
        Timer.Sample sample = this.metrics.recordCommitStart();
        this.metrics.recordCommitFailure(sample, WriteMetrics.CommitMode.GIT_DATA, WriteMetrics.FailureReason.STATUS_422);

        Timer timer = this.registry.find(WriteMetrics.METER_COMMITS_DURATION)
            .tags("mode", "git_data", "status", "failure")
            .timer();
        assertThat(timer, is(notNullValue()));
        assertThat(timer.count(), is(equalTo(1L)));

        Counter failureCounter = this.registry.find(WriteMetrics.METER_COMMITS_FAILURE)
            .tags("mode", "git_data", "reason", "422_non_ff")
            .counter();
        assertThat(failureCounter, is(notNullValue()));
        assertThat(failureCounter.count(), is(equalTo(1.0)));
    }

    @Test
    @DisplayName("git data step sample records per-step duration tagged by step")
    void gitDataStepSampleRecordsPerStepDuration() {
        Timer.Sample get = this.metrics.recordGitDataStepStart();
        this.metrics.recordGitDataStepEnd(get, WriteMetrics.GitDataStep.GET_REF);

        Timer.Sample blob = this.metrics.recordGitDataStepStart();
        this.metrics.recordGitDataStepEnd(blob, WriteMetrics.GitDataStep.CREATE_BLOB);

        Timer.Sample blob2 = this.metrics.recordGitDataStepStart();
        this.metrics.recordGitDataStepEnd(blob2, WriteMetrics.GitDataStep.CREATE_BLOB);

        assertThat(this.registry.find(WriteMetrics.METER_GIT_DATA_STEP_DURATION).tag("step", "get_ref").timer().count(), is(equalTo(1L)));
        assertThat(this.registry.find(WriteMetrics.METER_GIT_DATA_STEP_DURATION).tag("step", "create_blob").timer().count(), is(equalTo(2L)));
    }

    @Test
    @DisplayName("recordDispatchLatency captures producer-to-now duration")
    void recordDispatchLatency() {
        Instant producerTs = Instant.now().minus(Duration.ofMillis(50));
        this.metrics.recordDispatchLatency(producerTs);

        Timer timer = this.registry.find(WriteMetrics.METER_DISPATCH_LATENCY).timer();
        assertThat(timer, is(notNullValue()));
        assertThat(timer.count(), is(equalTo(1L)));
        assertThat(timer.totalTime(TimeUnit.MILLISECONDS), is(greaterThanOrEqualTo(40.0)));
    }

    @Test
    @DisplayName("recordEndToEndLatency captures buffered-to-now duration")
    void recordEndToEndLatency() {
        Instant bufferedAt = Instant.now().minus(Duration.ofMillis(25));
        this.metrics.recordEndToEndLatency(bufferedAt);

        Timer timer = this.registry.find(WriteMetrics.METER_END_TO_END_LATENCY).timer();
        assertThat(timer, is(notNullValue()));
        assertThat(timer.count(), is(equalTo(1L)));
        assertThat(timer.totalTime(TimeUnit.MILLISECONDS), is(greaterThanOrEqualTo(20.0)));
    }

    @Test
    @DisplayName("registerBufferDepthGauge creates a per-type gauge driven by the supplier")
    void registerBufferDepthGauge() {
        AtomicInteger size = new AtomicInteger(7);
        this.metrics.registerBufferDepthGauge(FakeModel.class, size::get);

        Gauge gauge = this.registry.find(WriteMetrics.METER_BUFFER_DEPTH).tag("type", FakeModel.class.getName()).gauge();
        assertThat(gauge, is(notNullValue()));
        assertThat(gauge.value(), is(equalTo(7.0)));

        size.set(42);
        assertThat(gauge.value(), is(equalTo(42.0)));
    }

    @Test
    @DisplayName("buffer depth gauges are bounded at one per distinct entity class")
    void bufferDepthGaugeCardinality() {
        AtomicInteger a = new AtomicInteger(1);
        AtomicInteger b = new AtomicInteger(2);
        this.metrics.registerBufferDepthGauge(FakeModel.class, a::get);
        this.metrics.registerBufferDepthGauge(OtherFakeModel.class, b::get);

        Collection<Meter> gauges = this.registry.find(WriteMetrics.METER_BUFFER_DEPTH).meters();
        assertThat(gauges, hasSize(2));

        for (Meter meter : gauges) {
            Tag tag = meter.getId().getTags().stream().filter(t -> t.getKey().equals("type")).findFirst().orElse(null);
            assertThat(tag, is(notNullValue()));
            assertThat(tag.getValue(), is(not(nullValue())));
        }
    }

    @Test
    @DisplayName("registerRetryImapSizeGauge samples Hazelcast IMap.size() via stub")
    void registerRetryImapSizeGauge() {
        HazelcastInstance instance = Mockito.mock(HazelcastInstance.class);
        @SuppressWarnings("unchecked")
        IMap<UUID, RetryEnvelope> retryMap = Mockito.mock(IMap.class);
        Mockito.when(instance.<UUID, RetryEnvelope>getMap(WriteQueueConsumer.RETRY_MAP_NAME)).thenReturn(retryMap);
        Mockito.when(retryMap.size()).thenReturn(3);

        this.metrics.registerRetryImapSizeGauge(instance);

        Gauge gauge = this.registry.find(WriteMetrics.METER_RETRY_IMAP_SIZE).gauge();
        assertThat(gauge, is(notNullValue()));
        assertThat(gauge.value(), is(equalTo(3.0)));
    }

    @Test
    @DisplayName("registerDeadletterImapSizeGauge samples Hazelcast IMap.size() via stub")
    void registerDeadletterImapSizeGauge() {
        HazelcastInstance instance = Mockito.mock(HazelcastInstance.class);
        @SuppressWarnings("unchecked")
        IMap<UUID, WriteRequest> deadletter = Mockito.mock(IMap.class);
        Mockito.when(instance.<UUID, WriteRequest>getMap(WriteQueueConsumer.DEADLETTER_MAP_NAME)).thenReturn(deadletter);
        Mockito.when(deadletter.size()).thenReturn(7);

        this.metrics.registerDeadletterImapSizeGauge(instance);

        Gauge gauge = this.registry.find(WriteMetrics.METER_DEADLETTER_IMAP_SIZE).gauge();
        assertThat(gauge, is(notNullValue()));
        assertThat(gauge.value(), is(equalTo(7.0)));
    }

    @Test
    @DisplayName("registerPrimaryQueueSizeGauge samples Hazelcast IQueue.size() via stub")
    void registerPrimaryQueueSizeGauge() {
        HazelcastInstance instance = Mockito.mock(HazelcastInstance.class);
        @SuppressWarnings("unchecked")
        IQueue<WriteRequest> queue = Mockito.mock(IQueue.class);
        Mockito.when(instance.<WriteRequest>getQueue(WriteQueueConsumer.QUEUE_NAME)).thenReturn(queue);
        Mockito.when(queue.size()).thenReturn(12);

        this.metrics.registerPrimaryQueueSizeGauge(instance);

        Gauge gauge = this.registry.find(WriteMetrics.METER_PRIMARY_QUEUE_SIZE).gauge();
        assertThat(gauge, is(notNullValue()));
        assertThat(gauge.value(), is(equalTo(12.0)));
    }

    @Test
    @DisplayName("FailureReason.fromStatus maps HTTP status codes to bounded tag values")
    void failureReasonFromStatus() {
        assertThat(WriteMetrics.FailureReason.fromStatus(422), is(equalTo(WriteMetrics.FailureReason.STATUS_422)));
        assertThat(WriteMetrics.FailureReason.fromStatus(409), is(equalTo(WriteMetrics.FailureReason.STATUS_409)));
        assertThat(WriteMetrics.FailureReason.fromStatus(500), is(equalTo(WriteMetrics.FailureReason.STATUS_5XX)));
        assertThat(WriteMetrics.FailureReason.fromStatus(503), is(equalTo(WriteMetrics.FailureReason.STATUS_5XX)));
        assertThat(WriteMetrics.FailureReason.fromStatus(401), is(equalTo(WriteMetrics.FailureReason.AUTH)));
        assertThat(WriteMetrics.FailureReason.fromStatus(403), is(equalTo(WriteMetrics.FailureReason.AUTH)));
        assertThat(WriteMetrics.FailureReason.fromStatus(418), is(equalTo(WriteMetrics.FailureReason.OTHER)));
        assertThat(WriteMetrics.FailureReason.fromStatus(404), is(equalTo(WriteMetrics.FailureReason.OTHER)));
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    /** Test-only {@link JpaModel} implementation for cardinality assertions. */
    private static class FakeModel implements JpaModel {
        @Serial
        private static final long serialVersionUID = 1L;
    }

    /** A second test-only {@link JpaModel} implementation so cardinality tests see 2 distinct types. */
    private static class OtherFakeModel implements JpaModel {
        @Serial
        private static final long serialVersionUID = 1L;
    }

}
