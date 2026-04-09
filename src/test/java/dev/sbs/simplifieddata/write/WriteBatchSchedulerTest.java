package dev.sbs.simplifieddata.write;

import dev.sbs.minecraftapi.MinecraftApi;
import dev.sbs.minecraftapi.persistence.model.ZodiacEvent;
import dev.sbs.simplifieddata.client.SkyBlockDataWriteContract;
import dev.sbs.simplifieddata.client.exception.SkyBlockDataException;
import dev.sbs.simplifieddata.client.request.PutContentRequest;
import dev.sbs.simplifieddata.client.response.GitHubContentEnvelope;
import dev.sbs.simplifieddata.client.response.GitHubPutResponse;
import dev.sbs.simplifieddata.persistence.RemoteSkyBlockFactory;
import dev.sbs.simplifieddata.persistence.WritableRemoteJsonSource;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.JpaRepository;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.source.IndexProvider;
import dev.simplified.persistence.source.ManifestIndex;
import dev.simplified.persistence.source.Source;
import dev.simplified.persistence.source.WriteRequest;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Unit tests for {@link WriteBatchScheduler} driven against a hand-rolled
 * {@link RemoteSkyBlockFactory} stub and a recording
 * {@link WriteQueueConsumer} stand-in. No Hazelcast, no Spring, no network
 * I/O.
 *
 * <p>Coverage matrix:
 * <ul>
 *   <li>{@code tick} with an empty registry is a no-op.</li>
 *   <li>{@code tick} with a successful {@code commitBatch} result does not
 *       escalate anything to the retry queue.</li>
 *   <li>{@code tick} with a failed {@code commitBatch} result schedules one
 *       retry entry per failed mutation, with attempt=1 and readyAt computed
 *       from the initial delay.</li>
 *   <li>{@code tick} with one successful source and one failed source
 *       handles them independently and reports the applied vs failed counts
 *       correctly.</li>
 *   <li>{@code flushOnShutdown} calls {@code commitBatch} on every source
 *       but does NOT escalate failures to the retry queue.</li>
 *   <li>A source whose {@code commitBatch} throws unexpectedly is skipped
 *       without breaking the iteration over other sources.</li>
 * </ul>
 */
class WriteBatchSchedulerTest {

    private RecordingConsumer consumer;
    private StubFactory factory;

    @BeforeEach
    void setUp() {
        this.consumer = new RecordingConsumer();
        this.factory = new StubFactory();
    }

    @Test
    @DisplayName("tick with an empty writable source registry is a no-op")
    void tickEmptyRegistry() {
        WriteBatchScheduler scheduler = new WriteBatchScheduler(this.factory, this.consumer, 1L);

        scheduler.tick();

        assertThat(this.consumer.getRetries(), is(empty()));
    }

    @Test
    @DisplayName("tick with a successful commitBatch result does not escalate retries")
    void tickAllSuccess() {
        RecordingSource<ZodiacEvent> source = new RecordingSource<>(ZodiacEvent.class);
        source.nextResult = WritableRemoteJsonSource.CommitBatchResult.success(2, "abc123");
        this.factory.register(ZodiacEvent.class, source);

        WriteBatchScheduler scheduler = new WriteBatchScheduler(this.factory, this.consumer, 1L);
        scheduler.tick();

        assertThat(source.commitCount, equalTo(1));
        assertThat(this.consumer.getRetries(), is(empty()));
    }

    @Test
    @DisplayName("tick escalates every failed mutation as a retry entry with attempt=1")
    void tickEscalatesFailures() {
        RecordingSource<ZodiacEvent> source = new RecordingSource<>(ZodiacEvent.class);
        BufferedMutation<ZodiacEvent> m1 = newMutation(WriteRequest.Operation.UPSERT, "A");
        BufferedMutation<ZodiacEvent> m2 = newMutation(WriteRequest.Operation.DELETE, "B");
        source.nextResult = buildFailed(List.of(m1, m2));
        this.factory.register(ZodiacEvent.class, source);

        WriteBatchScheduler scheduler = new WriteBatchScheduler(this.factory, this.consumer, 1L);
        scheduler.tick();

        assertThat(this.consumer.getRetries(), hasSize(2));
        for (DelayedWriteRequest retry : this.consumer.getRetries()) {
            assertThat(retry.getEntityType(), equalTo(ZodiacEvent.class));
            assertThat(retry.getAttempt(), equalTo(1));
            assertThat(retry.getReadyAt(), notNullValue());
            // Verify that the readyAt is at least close to now + 1 minute (with some tolerance for scheduling jitter).
            long diffSeconds = java.time.Duration.between(Instant.now(), retry.getReadyAt()).toSeconds();
            assertThat(diffSeconds > 50 && diffSeconds < 70, is(true));
        }
    }

    @Test
    @DisplayName("tick with a successful source and a failed source handles each independently")
    void tickMixedResults() {
        RecordingSource<ZodiacEvent> okSource = new RecordingSource<>(ZodiacEvent.class);
        okSource.nextResult = WritableRemoteJsonSource.CommitBatchResult.success(3, "abc");
        this.factory.register(ZodiacEvent.class, okSource);

        RecordingSource<OtherModel> badSource = new RecordingSource<>(OtherModel.class);
        badSource.nextResult = buildFailed(List.of(newOtherMutation()));
        this.factory.register(OtherModel.class, badSource);

        WriteBatchScheduler scheduler = new WriteBatchScheduler(this.factory, this.consumer, 1L);
        scheduler.tick();

        assertThat(okSource.commitCount, equalTo(1));
        assertThat(badSource.commitCount, equalTo(1));
        assertThat(this.consumer.getRetries(), hasSize(1));
        assertThat(this.consumer.getRetries().get(0).getEntityType(), equalTo(OtherModel.class));
    }

    @Test
    @DisplayName("flushOnShutdown commits every source but does not escalate failures")
    void flushOnShutdownDropsFailures() {
        RecordingSource<ZodiacEvent> source = new RecordingSource<>(ZodiacEvent.class);
        source.nextResult = buildFailed(List.of(newMutation(WriteRequest.Operation.UPSERT, "A")));
        this.factory.register(ZodiacEvent.class, source);

        WriteBatchScheduler scheduler = new WriteBatchScheduler(this.factory, this.consumer, 1L);
        scheduler.flushOnShutdown();

        assertThat(source.commitCount, equalTo(1));
        assertThat(this.consumer.getRetries(), is(empty()));
    }

    @Test
    @DisplayName("source that throws from commitBatch is skipped without stopping the iteration")
    void throwingSourceSkipped() {
        ThrowingSource throwing = new ThrowingSource();
        this.factory.register(ZodiacEvent.class, throwing);

        RecordingSource<OtherModel> other = new RecordingSource<>(OtherModel.class);
        other.nextResult = WritableRemoteJsonSource.CommitBatchResult.success(1, "xyz");
        this.factory.register(OtherModel.class, other);

        WriteBatchScheduler scheduler = new WriteBatchScheduler(this.factory, this.consumer, 1L);
        scheduler.tick();

        assertThat(throwing.commitCount, equalTo(1));
        assertThat(other.commitCount, equalTo(1));
        assertThat(this.consumer.getRetries(), is(empty()));
    }

    // --- helpers --- //

    private static @NotNull BufferedMutation<ZodiacEvent> newMutation(@NotNull WriteRequest.Operation op, @NotNull String id) {
        ZodiacEvent event = new ZodiacEvent();
        return new BufferedMutation<>(op, event, UUID.randomUUID(), Instant.now());
    }

    private static @NotNull BufferedMutation<OtherModel> newOtherMutation() {
        return new BufferedMutation<>(WriteRequest.Operation.UPSERT, new OtherModel(), UUID.randomUUID(), Instant.now());
    }

    @SuppressWarnings("unchecked")
    private static @NotNull WritableRemoteJsonSource.CommitBatchResult buildFailed(@NotNull List<BufferedMutation<? extends JpaModel>> failures) {
        Collection<BufferedMutation<?>> casted = (Collection<BufferedMutation<?>>) (Collection<?>) failures;
        return WritableRemoteJsonSource.CommitBatchResult.failed(casted, new RuntimeException("stub failure"));
    }

    /** Marker no-Id model used to drive the mixed-results test case. */
    @jakarta.persistence.Entity
    @jakarta.persistence.Table(name = "other_model")
    static final class OtherModel implements JpaModel {

        @jakarta.persistence.Id
        @jakarta.persistence.Column(name = "id")
        private @NotNull String id = "default";

    }

    // --- stubs --- //

    private static final class StubFactory extends RemoteSkyBlockFactory {

        StubFactory() {
            super(
                "stub",
                new EmptySkyBlockFactory(),
                () -> { throw new UnsupportedOperationException(); },
                path -> { throw new UnsupportedOperationException(); },
                new ThrowingContract(),
                MinecraftApi.getGson(),
                java.nio.file.Path.of("target/stub-overlay-does-not-exist"),
                3
            );
        }

        void register(@NotNull Class<? extends JpaModel> type, @NotNull WritableRemoteJsonSource<?> source) {
            this.getWritableSources().put(type, source);
            this.getSources().put(type, source);
        }

    }

    /**
     * {@link dev.sbs.minecraftapi.persistence.SkyBlockFactory} whose
     * {@code getModels()} returns an empty list so {@link RemoteSkyBlockFactory}'s
     * constructor does not try to wire a source per real SkyBlock entity
     * (which would fail because the stubbed index provider + file fetcher +
     * write contract throw on every invocation).
     */
    private static final class EmptySkyBlockFactory extends dev.sbs.minecraftapi.persistence.SkyBlockFactory {

        @Override
        public @NotNull ConcurrentList<Class<JpaModel>> getModels() {
            return Concurrent.newUnmodifiableList();
        }

    }

    /**
     * Recording stand-in for {@link WritableRemoteJsonSource}. Does NOT extend
     * the real class because tests only need to hand back a pre-computed
     * {@code CommitBatchResult}. A straight subclass is simpler than a mock
     * framework.
     */
    private static class RecordingSource<T extends JpaModel> extends WritableRemoteJsonSource<T> {

        WritableRemoteJsonSource.CommitBatchResult nextResult = WritableRemoteJsonSource.CommitBatchResult.empty();
        int commitCount = 0;

        RecordingSource(@NotNull Class<T> modelClass) {
            super(
                new NoopDelegate<>(),
                new ThrowingContract(),
                new EmptyIndexProvider(),
                MinecraftApi.getGson(),
                "stub-" + modelClass.getSimpleName(),
                modelClass,
                3
            );
        }

        @Override
        public @NotNull CommitBatchResult commitBatch() {
            this.commitCount++;
            return this.nextResult;
        }

    }

    private static final class ThrowingSource extends RecordingSource<ZodiacEvent> {

        ThrowingSource() {
            super(ZodiacEvent.class);
        }

        @Override
        public @NotNull CommitBatchResult commitBatch() {
            this.commitCount++;
            throw new IllegalStateException("simulated commitBatch failure");
        }

    }

    private static final class RecordingConsumer extends WriteQueueConsumer {

        private final List<DelayedWriteRequest> retries = new ArrayList<>();

        RecordingConsumer() {
            super(noopHazelcastProxy(), new StubFactory(), 1L, 5, false);
        }

        @Override
        public void scheduleRetry(@NotNull DelayedWriteRequest delayed) {
            this.retries.add(delayed);
        }

        List<DelayedWriteRequest> getRetries() {
            return this.retries;
        }

    }

    /**
     * Dynamic proxy that implements {@link com.hazelcast.core.HazelcastInstance}
     * by throwing on every method call. Used by the recording consumer's
     * constructor - the tests never call any method on the Hazelcast
     * instance because they invoke {@code scheduleRetry} directly and the
     * drain thread is disabled via the {@code enabled=false} flag.
     */
    private static com.hazelcast.core.@NotNull HazelcastInstance noopHazelcastProxy() {
        return (com.hazelcast.core.HazelcastInstance) java.lang.reflect.Proxy.newProxyInstance(
            com.hazelcast.core.HazelcastInstance.class.getClassLoader(),
            new Class<?>[] { com.hazelcast.core.HazelcastInstance.class },
            (proxy, method, args) -> {
                if ("shutdown".equals(method.getName()))
                    return null;
                throw new UnsupportedOperationException("noop HazelcastInstance proxy called: " + method.getName());
            }
        );
    }

    private static final class NoopDelegate<T extends JpaModel> implements Source<T> {

        @Override
        public @NotNull ConcurrentList<T> load(@NotNull JpaRepository<T> repository) {
            return Concurrent.newList();
        }

    }

    private static final class EmptyIndexProvider implements IndexProvider {

        @Override
        public @NotNull ManifestIndex loadIndex() {
            return ManifestIndex.empty();
        }

    }

    private static final class ThrowingContract implements SkyBlockDataWriteContract {

        @Override
        public @NotNull GitHubContentEnvelope getFileMetadata(@NotNull String path) throws SkyBlockDataException {
            throw new UnsupportedOperationException();
        }

        @Override
        public @NotNull GitHubPutResponse putFileContent(@NotNull String path, @NotNull PutContentRequest body) throws SkyBlockDataException {
            throw new UnsupportedOperationException();
        }

    }

}
