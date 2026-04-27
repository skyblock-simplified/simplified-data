package dev.sbs.simplifieddata.write;

import dev.sbs.simplifieddata.DataApi;
import dev.sbs.simplifieddata.client.SkyBlockDataWriteContract;
import dev.sbs.simplifieddata.client.exception.SkyBlockDataException;
import dev.sbs.simplifieddata.client.request.PutContentRequest;
import dev.sbs.simplifieddata.client.response.GitHubContentEnvelope;
import dev.sbs.simplifieddata.client.response.GitHubPutResponse;
import dev.sbs.simplifieddata.persistence.RemoteSkyBlockFactory;
import dev.sbs.simplifieddata.persistence.WritableRemoteJsonSource;
import dev.sbs.skyblockdata.model.ZodiacEvent;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.JpaRepository;
import dev.simplified.persistence.source.IndexProvider;
import dev.simplified.persistence.source.ManifestIndex;
import dev.simplified.persistence.source.Source;
import dev.simplified.persistence.source.WriteRequest;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

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
    private WriteMetrics metrics;

    @BeforeEach
    void setUp() {
        this.consumer = new RecordingConsumer();
        this.factory = new StubFactory();
        this.metrics = new WriteMetrics(new SimpleMeterRegistry());
    }

    @Test
    @DisplayName("tick with an empty writable source registry is a no-op")
    void tickEmptyRegistry() {
        WriteBatchScheduler scheduler = new WriteBatchScheduler(this.factory, this.consumer, stubGitDataService(), this.metrics, 1L, WriteMode.CONTENTS);

        scheduler.tick();

        assertThat(this.consumer.getRetries(), is(empty()));
    }

    @Test
    @DisplayName("tick with a successful commitBatch result does not escalate retries")
    void tickAllSuccess() {
        RecordingSource<ZodiacEvent> source = new RecordingSource<>(ZodiacEvent.class);
        source.nextResult = WritableRemoteJsonSource.CommitBatchResult.success(2, "abc123");
        this.factory.register(ZodiacEvent.class, source);

        WriteBatchScheduler scheduler = new WriteBatchScheduler(this.factory, this.consumer, stubGitDataService(), this.metrics, 1L, WriteMode.CONTENTS);
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

        WriteBatchScheduler scheduler = new WriteBatchScheduler(this.factory, this.consumer, stubGitDataService(), this.metrics, 1L, WriteMode.CONTENTS);
        scheduler.tick();

        assertThat(this.consumer.getRetries(), hasSize(2));
        long nowMillis = Instant.now().toEpochMilli();
        for (RetryEnvelope retry : this.consumer.getRetries()) {
            assertThat(retry.getRequest().getEntityClassName(), equalTo(ZodiacEvent.class.getName()));
            assertThat(retry.getAttempt(), equalTo(1));
            // Verify that the readyAt is at least close to now + 1 minute (with some tolerance for scheduling jitter).
            long diffSeconds = (retry.getReadyAtEpochMillis() - nowMillis) / 1000L;
            assertThat(diffSeconds > 50 && diffSeconds < 70, is(true));
        }
    }

    @Test
    @DisplayName("Phase 6b.1 Gap 1: a mutation with attempt=2 escalates to attempt=3 (not 1)")
    void tickEscalatesIncrementsAttemptCounter() {
        RecordingSource<ZodiacEvent> source = new RecordingSource<>(ZodiacEvent.class);
        // Build a BufferedMutation that was already on its 2nd retry cycle.
        ZodiacEvent event = new ZodiacEvent();
        BufferedMutation<ZodiacEvent> priorRetry = new BufferedMutation<>(
            WriteRequest.Operation.UPSERT,
            event,
            UUID.randomUUID(),
            Instant.now(),
            2  // attempt counter: this mutation has already failed twice
        );
        source.nextResult = buildFailed(List.of(priorRetry));
        this.factory.register(ZodiacEvent.class, source);

        WriteBatchScheduler scheduler = new WriteBatchScheduler(this.factory, this.consumer, stubGitDataService(), this.metrics, 1L, WriteMode.CONTENTS);
        scheduler.tick();

        assertThat(this.consumer.getRetries(), hasSize(1));
        // The next retry must be attempt=3 (2 + 1), not attempt=1.
        assertThat(this.consumer.getRetries().getFirst().getAttempt(), equalTo(3));
        // Exponential-backoff readyAt for attempt=3 is now + 1min * 2^(3-1) = now + 4min.
        long nowMillis = Instant.now().toEpochMilli();
        long diffSeconds = (this.consumer.getRetries().getFirst().getReadyAtEpochMillis() - nowMillis) / 1000L;
        assertThat(diffSeconds > 230 && diffSeconds < 250, is(true));
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

        WriteBatchScheduler scheduler = new WriteBatchScheduler(this.factory, this.consumer, stubGitDataService(), this.metrics, 1L, WriteMode.CONTENTS);
        scheduler.tick();

        assertThat(okSource.commitCount, equalTo(1));
        assertThat(badSource.commitCount, equalTo(1));
        assertThat(this.consumer.getRetries(), hasSize(1));
        assertThat(this.consumer.getRetries().getFirst().getRequest().getEntityClassName(), equalTo(OtherModel.class.getName()));
    }

    @Test
    @DisplayName("flushOnShutdown commits every source but does not escalate failures")
    void flushOnShutdownDropsFailures() {
        RecordingSource<ZodiacEvent> source = new RecordingSource<>(ZodiacEvent.class);
        source.nextResult = buildFailed(List.of(newMutation(WriteRequest.Operation.UPSERT, "A")));
        this.factory.register(ZodiacEvent.class, source);

        WriteBatchScheduler scheduler = new WriteBatchScheduler(this.factory, this.consumer, stubGitDataService(), this.metrics, 1L, WriteMode.CONTENTS);
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

        WriteBatchScheduler scheduler = new WriteBatchScheduler(this.factory, this.consumer, stubGitDataService(), this.metrics, 1L, WriteMode.CONTENTS);
        scheduler.tick();

        assertThat(throwing.commitCount, equalTo(1));
        assertThat(other.commitCount, equalTo(1));
        assertThat(this.consumer.getRetries(), is(empty()));
    }

    // --- GIT_DATA mode tests --- //

    @Test
    @DisplayName("GIT_DATA tick with empty registry is a no-op and never invokes the commit service")
    void gitDataTickEmpty() {
        CapturingGitDataService capturing = new CapturingGitDataService();
        WriteBatchScheduler scheduler = new WriteBatchScheduler(this.factory, this.consumer, capturing, this.metrics, 1L, WriteMode.GIT_DATA);

        scheduler.tick();

        assertThat(capturing.commitCalls, hasSize(0));
        assertThat(this.consumer.getRetries(), is(empty()));
    }

    @Test
    @DisplayName("GIT_DATA tick stages dirty sources then calls commit() once with merged request")
    void gitDataTickMergesSources() {
        StagingRecordingSource<ZodiacEvent> zodiac = new StagingRecordingSource<>(ZodiacEvent.class);
        zodiac.nextStaged = newStagedBatch(ZodiacEvent.class, "data/v1/world/zodiac_events.json", 2);
        this.factory.register(ZodiacEvent.class, zodiac);

        StagingRecordingSource<OtherModel> other = new StagingRecordingSource<>(OtherModel.class);
        other.nextStaged = newStagedBatch(OtherModel.class, "data/v1/other/other.json", 3);
        this.factory.register(OtherModel.class, other);

        CapturingGitDataService capturing = new CapturingGitDataService();
        capturing.nextResult = GitDataCommitResult.success("commitsha");
        WriteBatchScheduler scheduler = new WriteBatchScheduler(this.factory, this.consumer, capturing, this.metrics, 1L, WriteMode.GIT_DATA);

        scheduler.tick();

        assertThat(capturing.commitCalls, hasSize(1));
        BatchCommitRequest merged = capturing.commitCalls.getFirst();
        assertThat(merged.getDirtyFileCount(), equalTo(2));
        assertThat(merged.getTotalMutationCount(), equalTo(5));
        assertThat(this.consumer.getRetries(), is(empty()));
    }

    @Test
    @DisplayName("GIT_DATA tick escalates every mutation from every source when the commit service returns failure")
    void gitDataTickEscalatesOnFailure() {
        StagingRecordingSource<ZodiacEvent> zodiac = new StagingRecordingSource<>(ZodiacEvent.class);
        zodiac.nextStaged = newStagedBatch(ZodiacEvent.class, "data/v1/world/zodiac_events.json", 2);
        this.factory.register(ZodiacEvent.class, zodiac);

        StagingRecordingSource<OtherModel> other = new StagingRecordingSource<>(OtherModel.class);
        other.nextStaged = newStagedBatch(OtherModel.class, "data/v1/other/other.json", 3);
        this.factory.register(OtherModel.class, other);

        CapturingGitDataService capturing = new CapturingGitDataService();
        capturing.nextResult = GitDataCommitResult.failure(new RuntimeException("simulated Git Data failure"));
        WriteBatchScheduler scheduler = new WriteBatchScheduler(this.factory, this.consumer, capturing, this.metrics, 1L, WriteMode.GIT_DATA);

        scheduler.tick();

        assertThat(capturing.commitCalls, hasSize(1));
        // 2 ZodiacEvent mutations + 3 OtherModel mutations = 5 retry entries.
        assertThat(this.consumer.getRetries(), hasSize(5));
    }

    @Test
    @DisplayName("GIT_DATA tick skips empty StagedBatches without invoking commit()")
    void gitDataTickSkipsEmptyStagedBatches() {
        StagingRecordingSource<ZodiacEvent> zodiac = new StagingRecordingSource<>(ZodiacEvent.class);
        zodiac.nextStaged = StagedBatch.empty();
        this.factory.register(ZodiacEvent.class, zodiac);

        CapturingGitDataService capturing = new CapturingGitDataService();
        WriteBatchScheduler scheduler = new WriteBatchScheduler(this.factory, this.consumer, capturing, this.metrics, 1L, WriteMode.GIT_DATA);

        scheduler.tick();

        assertThat(capturing.commitCalls, hasSize(0));
    }

    @Test
    @DisplayName("GIT_DATA flushOnShutdown drops failures instead of escalating them")
    void gitDataFlushDropsFailures() {
        StagingRecordingSource<ZodiacEvent> zodiac = new StagingRecordingSource<>(ZodiacEvent.class);
        zodiac.nextStaged = newStagedBatch(ZodiacEvent.class, "data/v1/world/zodiac_events.json", 2);
        this.factory.register(ZodiacEvent.class, zodiac);

        CapturingGitDataService capturing = new CapturingGitDataService();
        capturing.nextResult = GitDataCommitResult.failure(new RuntimeException("simulated failure"));
        WriteBatchScheduler scheduler = new WriteBatchScheduler(this.factory, this.consumer, capturing, this.metrics, 1L, WriteMode.GIT_DATA);

        scheduler.flushOnShutdown();

        assertThat(capturing.commitCalls, hasSize(1));
        assertThat(this.consumer.getRetries(), is(empty()));
    }

    // --- helpers --- //

    /**
     * Stub GitDataCommitService that throws if any commit() call reaches it.
     * Used by the legacy CONTENTS-mode tests which should never dispatch
     * through the git-data path.
     */
    private static @NotNull GitDataCommitService stubGitDataService() {
        SkyBlockGitDataContractStub contractStub = new SkyBlockGitDataContractStub();
        return new GitDataCommitService(contractStub, new WriteMetrics(new SimpleMeterRegistry()), 3) {
            @Override
            public @NotNull GitDataCommitResult commit(@NotNull BatchCommitRequest request) {
                throw new UnsupportedOperationException(
                    "stubGitDataService.commit() reached from CONTENTS-mode test - unexpected dispatch"
                );
            }
        };
    }

    private static @NotNull BufferedMutation<ZodiacEvent> newMutation(@NotNull WriteRequest.Operation op, @NotNull String id) {
        ZodiacEvent event = new ZodiacEvent();
        return new BufferedMutation<>(op, event, UUID.randomUUID(), Instant.now());
    }

    private static <T extends JpaModel> @NotNull StagedBatch<T> newStagedBatch(
        @NotNull Class<T> modelClass,
        @NotNull String path,
        int mutationCount
    ) {
        dev.simplified.collection.ConcurrentMap<String, ConcurrentList<T>> fileSnapshots = Concurrent.newMap();
        ConcurrentList<T> entities = Concurrent.newList();
        fileSnapshots.put(path, entities);

        ConcurrentList<BufferedMutation<T>> mutations = Concurrent.newList();
        for (int i = 0; i < mutationCount; i++) {
            T instance = instantiate(modelClass);
            mutations.add(new BufferedMutation<>(WriteRequest.Operation.UPSERT, instance, UUID.randomUUID(), Instant.now()));
        }

        return new StagedBatch<>(modelClass, fileSnapshots, mutations, mutationCount);
    }

    private static <T extends JpaModel> @NotNull T instantiate(@NotNull Class<T> type) {
        try {
            return type.getDeclaredConstructor().newInstance();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private static @NotNull BufferedMutation<OtherModel> newOtherMutation() {
        return new BufferedMutation<>(WriteRequest.Operation.UPSERT, new OtherModel(), UUID.randomUUID(), Instant.now());
    }

    private static @NotNull WritableRemoteJsonSource.CommitBatchResult buildFailed(@NotNull List<BufferedMutation<? extends JpaModel>> failures) {
        return WritableRemoteJsonSource.CommitBatchResult.failed(failures, new RuntimeException("stub failure"));
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
                DataApi.getGson(),
                java.nio.file.Path.of("target/stub-overlay-does-not-exist"),
                3,
                new WriteMetrics(new SimpleMeterRegistry())
            );
        }

        void register(@NotNull Class<? extends JpaModel> type, @NotNull WritableRemoteJsonSource<?> source) {
            this.getWritableSources().put(type, source);
            this.getSources().put(type, source);
        }

    }

    /**
     * {@link dev.sbs.skyblockdata.SkyBlockFactory} whose
     * {@code getModels()} returns an empty list so {@link RemoteSkyBlockFactory}'s
     * constructor does not try to wire a source per real SkyBlock entity
     * (which would fail because the stubbed index provider + file fetcher +
     * write contract throw on every invocation).
     */
    private static final class EmptySkyBlockFactory extends dev.sbs.skyblockdata.SkyBlockFactory {

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
                new ThrowingFileFetcher(),
                new EmptyIndexProvider(),
                DataApi.getGson(),
                "stub-" + modelClass.getSimpleName(),
                modelClass,
                3,
                new WriteMetrics(new SimpleMeterRegistry())
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

    /**
     * Stand-in for {@link WritableRemoteJsonSource} whose {@code stageBatch()}
     * returns a pre-computed {@link StagedBatch}. Used by the GIT_DATA mode
     * tests to drive the scheduler's staging phase without exercising the
     * real manifest/file fetcher plumbing.
     */
    private static final class StagingRecordingSource<T extends JpaModel> extends WritableRemoteJsonSource<T> {

        @NotNull StagedBatch<T> nextStaged = StagedBatch.empty();
        int stageCount = 0;

        StagingRecordingSource(@NotNull Class<T> modelClass) {
            super(
                new NoopDelegate<>(),
                new ThrowingContract(),
                new ThrowingFileFetcher(),
                new EmptyIndexProvider(),
                DataApi.getGson(),
                "staging-" + modelClass.getSimpleName(),
                modelClass,
                3,
                new WriteMetrics(new SimpleMeterRegistry())
            );
        }

        @Override
        public @NotNull StagedBatch<T> stageBatch() {
            this.stageCount++;
            return this.nextStaged;
        }

    }

    /**
     * Stand-in for {@link GitDataCommitService} that records every
     * {@link #commit(BatchCommitRequest)} call and returns a pre-configured
     * {@link GitDataCommitResult}.
     */
    private static final class CapturingGitDataService extends GitDataCommitService {

        final List<BatchCommitRequest> commitCalls = new ArrayList<>();
        @NotNull GitDataCommitResult nextResult = GitDataCommitResult.success("stub-commit-sha");

        CapturingGitDataService() {
            super(new SkyBlockGitDataContractStub(), new WriteMetrics(new SimpleMeterRegistry()), 3);
        }

        @Override
        public @NotNull GitDataCommitResult commit(@NotNull BatchCommitRequest request) {
            this.commitCalls.add(request);
            return this.nextResult;
        }

    }

    private static final class RecordingConsumer extends WriteQueueConsumer {

        private final List<RetryEnvelope> retries = new ArrayList<>();

        RecordingConsumer() {
            super(noopHazelcastProxy(), new StubFactory(), new WriteMetrics(new SimpleMeterRegistry()), 1L, 5, false);
        }

        @Override
        public void scheduleRetry(@NotNull RetryEnvelope envelope) {
            this.retries.add(envelope);
        }

        List<RetryEnvelope> getRetries() {
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

    private static final class ThrowingFileFetcher implements dev.simplified.persistence.source.FileFetcher {

        @Override
        public @NotNull String fetchFile(@NotNull String path) {
            throw new UnsupportedOperationException("FileFetcher stub should not be invoked");
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

    /**
     * Minimal {@link dev.sbs.simplifieddata.client.SkyBlockGitDataContract}
     * stub that throws on every method. Only used by the
     * {@link #stubGitDataService()} helper to satisfy the
     * {@link GitDataCommitService} constructor parameter in legacy
     * CONTENTS-mode tests. Never actually invoked because the
     * {@link GitDataCommitService} subclass override intercepts all
     * {@link GitDataCommitService#commit} calls.
     */
    private static final class SkyBlockGitDataContractStub implements dev.sbs.simplifieddata.client.SkyBlockGitDataContract {

        @Override
        public dev.sbs.simplifieddata.client.response.@NotNull GitRef getRef(@NotNull String branch) {
            throw new UnsupportedOperationException();
        }

        @Override
        public dev.sbs.simplifieddata.client.response.@NotNull GitCommit getCommit(@NotNull String sha) {
            throw new UnsupportedOperationException();
        }

        @Override
        public dev.sbs.simplifieddata.client.response.@NotNull GitTree getTree(@NotNull String sha, @NotNull String recursive) {
            throw new UnsupportedOperationException();
        }

        @Override
        public dev.sbs.simplifieddata.client.response.@NotNull GitBlob createBlob(dev.sbs.simplifieddata.client.request.@NotNull CreateBlobRequest body) {
            throw new UnsupportedOperationException();
        }

        @Override
        public dev.sbs.simplifieddata.client.response.@NotNull GitTree createTree(dev.sbs.simplifieddata.client.request.@NotNull CreateTreeRequest body) {
            throw new UnsupportedOperationException();
        }

        @Override
        public dev.sbs.simplifieddata.client.response.@NotNull GitCommit createCommit(dev.sbs.simplifieddata.client.request.@NotNull CreateCommitRequest body) {
            throw new UnsupportedOperationException();
        }

        @Override
        public dev.sbs.simplifieddata.client.response.@NotNull GitRef updateRef(@NotNull String branch, dev.sbs.simplifieddata.client.request.@NotNull UpdateRefRequest body) {
            throw new UnsupportedOperationException();
        }

    }

}
