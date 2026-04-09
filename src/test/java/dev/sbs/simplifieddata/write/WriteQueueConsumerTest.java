package dev.sbs.simplifieddata.write;

import com.hazelcast.collection.IQueue;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
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
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.JpaRepository;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.source.IndexProvider;
import dev.simplified.persistence.source.ManifestIndex;
import dev.simplified.persistence.source.Source;
import dev.simplified.persistence.source.WriteRequest;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * Integration test for {@link WriteQueueConsumer} driven against a real
 * in-process Hazelcast 5.6 member. Boots a single-member isolated cluster
 * per test so the queue and dead-letter map are clean across cases, wires a
 * stub {@link RemoteSkyBlockFactory} carrying a recording
 * {@link WritableRemoteJsonSource} stand-in, and exercises the drain loop by
 * putting synthetic {@link WriteRequest} entries on the Hazelcast
 * {@link IQueue}.
 *
 * <p>The test uses {@link Hazelcast#newHazelcastInstance(Config)} with
 * discovery disabled (no multicast, no TCP/IP join) and a random free
 * port, matching the pattern used by the persistence library's
 * {@code JpaCacheHazelcastTest}.
 */
@Tag("slow")
class WriteQueueConsumerTest {

    private HazelcastInstance hazelcast;
    private StubFactory factory;
    private RecordingSource<ZodiacEvent> recordingSource;

    @BeforeEach
    void setUp() {
        Config config = new Config();
        config.setClusterName("write-queue-consumer-test-" + UUID.randomUUID());

        NetworkConfig network = config.getNetworkConfig();
        network.setPortAutoIncrement(true);
        network.setPort(0); // Random free port
        network.setPortCount(100);
        network.setReuseAddress(true);

        JoinConfig join = network.getJoin();
        join.getAutoDetectionConfig().setEnabled(false);
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);

        this.hazelcast = Hazelcast.newHazelcastInstance(config);
        this.recordingSource = new RecordingSource<>(ZodiacEvent.class);
        this.factory = new StubFactory();
        this.factory.register(ZodiacEvent.class, this.recordingSource);
    }

    @AfterEach
    void tearDown() {
        if (this.hazelcast != null)
            this.hazelcast.shutdown();
    }

    @Test
    @DisplayName("drain loop dispatches an upsert WriteRequest from the IQueue to the matching source")
    void drainUpsert() throws Exception {
        WriteQueueConsumer consumer = newConsumer();

        ZodiacEvent event = newEvent("YEAR_OF_THE_SEAL", "Year of the Seal", 414);
        WriteRequest request = WriteRequest.upsert(ZodiacEvent.class, event, MinecraftApi.getGson(), "skyblock-data");

        IQueue<WriteRequest> queue = this.hazelcast.getQueue(WriteQueueConsumer.QUEUE_NAME);
        queue.put(request);

        consumer.start();
        waitUntil(() -> !this.recordingSource.bufferedMutations.isEmpty(), Duration.ofSeconds(5));
        consumer.stop();

        assertThat(this.recordingSource.bufferedMutations, hasSize(1));
        BufferedMutation<ZodiacEvent> mutation = this.recordingSource.bufferedMutations.get(0);
        assertThat(mutation.getOperation(), equalTo(WriteRequest.Operation.UPSERT));
        assertThat(mutation.getEntity().getId(), equalTo("YEAR_OF_THE_SEAL"));
        assertThat(mutation.getRequestId(), equalTo(request.getRequestId()));
    }

    @Test
    @DisplayName("drain loop dispatches a delete WriteRequest from the IQueue to the matching source")
    void drainDelete() throws Exception {
        WriteQueueConsumer consumer = newConsumer();

        ZodiacEvent event = newEvent("YEAR_OF_THE_WHALE", "Year of the Whale", 413);
        WriteRequest request = WriteRequest.delete(ZodiacEvent.class, event, MinecraftApi.getGson(), "skyblock-data");

        IQueue<WriteRequest> queue = this.hazelcast.getQueue(WriteQueueConsumer.QUEUE_NAME);
        queue.put(request);

        consumer.start();
        waitUntil(() -> !this.recordingSource.bufferedMutations.isEmpty(), Duration.ofSeconds(5));
        consumer.stop();

        assertThat(this.recordingSource.bufferedMutations, hasSize(1));
        assertThat(this.recordingSource.bufferedMutations.get(0).getOperation(), equalTo(WriteRequest.Operation.DELETE));
    }

    @Test
    @DisplayName("scheduleRetry with attempt beyond cap dead-letters directly to the IMap")
    void deadLetterPastCap() throws Exception {
        WriteQueueConsumer consumer = newConsumer();

        ZodiacEvent event = newEvent("YEAR_OF_THE_DOLPHIN", "Year of the Dolphin", 415);
        BufferedMutation<ZodiacEvent> mutation = new BufferedMutation<>(
            WriteRequest.Operation.UPSERT, event, UUID.randomUUID(), Instant.now()
        );
        DelayedWriteRequest delayed = new DelayedWriteRequest(ZodiacEvent.class, mutation, 10, Instant.now());

        consumer.scheduleRetry(delayed);

        IMap<UUID, WriteRequest> deadletter = this.hazelcast.getMap(WriteQueueConsumer.DEADLETTER_MAP_NAME);
        assertThat(deadletter.size(), equalTo(1));
        assertThat(deadletter.containsKey(mutation.getRequestId()), is(true));
    }

    @Test
    @DisplayName("scheduleRetry within cap inserts into the retry queue without touching the dead-letter map")
    void retryWithinCap() throws Exception {
        WriteQueueConsumer consumer = newConsumer();

        ZodiacEvent event = newEvent("YEAR_OF_THE_OCTOPUS", "Year of the Octopus", 416);
        BufferedMutation<ZodiacEvent> mutation = new BufferedMutation<>(
            WriteRequest.Operation.UPSERT, event, UUID.randomUUID(), Instant.now()
        );
        // Set readyAt far in the past so the retry is immediately eligible.
        DelayedWriteRequest delayed = new DelayedWriteRequest(
            ZodiacEvent.class, mutation, 1, Instant.now().minusSeconds(1)
        );

        consumer.scheduleRetry(delayed);

        IMap<UUID, WriteRequest> deadletter = this.hazelcast.getMap(WriteQueueConsumer.DEADLETTER_MAP_NAME);
        assertThat(deadletter.size(), equalTo(0));

        // Start the consumer so it drains the retry queue.
        consumer.start();
        waitUntil(() -> !this.recordingSource.bufferedMutations.isEmpty(), Duration.ofSeconds(5));
        consumer.stop();

        assertThat(this.recordingSource.bufferedMutations, hasSize(1));
        assertThat(this.recordingSource.bufferedMutations.get(0).getRequestId(), equalTo(mutation.getRequestId()));
    }

    @Test
    @DisplayName("WriteRequest for an unregistered type is skipped without stopping the drain loop")
    void unknownTypeSkipped() throws Exception {
        // Clear the factory so ZodiacEvent is unregistered.
        this.factory.clear();

        WriteQueueConsumer consumer = newConsumer();

        ZodiacEvent event = newEvent("YEAR_OF_THE_SEAL", "Year of the Seal", 414);
        WriteRequest skipped = WriteRequest.upsert(ZodiacEvent.class, event, MinecraftApi.getGson(), "skyblock-data");

        IQueue<WriteRequest> queue = this.hazelcast.getQueue(WriteQueueConsumer.QUEUE_NAME);
        queue.put(skipped);

        consumer.start();
        // Wait a moment for the drain loop to consume + skip.
        Thread.sleep(1000);
        consumer.stop();

        assertThat(this.recordingSource.bufferedMutations, hasSize(0));
        // Queue must be drained even for unrecognized entries.
        assertThat(queue.size(), equalTo(0));
    }

    // --- helpers --- //

    private @NotNull WriteQueueConsumer newConsumer() {
        return new WriteQueueConsumer(this.hazelcast, this.factory, 1L, 3, true);
    }

    private static @NotNull ZodiacEvent newEvent(@NotNull String id, @NotNull String name, int releaseYear) {
        ZodiacEvent event = new ZodiacEvent();
        setField(event, "id", id);
        setField(event, "name", name);
        setField(event, "releaseYear", releaseYear);
        return event;
    }

    private static void setField(@NotNull Object target, @NotNull String fieldName, Object value) {
        try {
            java.lang.reflect.Field field = ZodiacEvent.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(target, value);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private static void waitUntil(@NotNull java.util.function.BooleanSupplier condition, @NotNull Duration timeout) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean())
                return;
            Thread.sleep(50);
        }
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

        void clear() {
            this.getWritableSources().clear();
            this.getSources().clear();
        }

    }

    private static final class EmptySkyBlockFactory extends dev.sbs.minecraftapi.persistence.SkyBlockFactory {

        @Override
        public @NotNull ConcurrentList<Class<JpaModel>> getModels() {
            return Concurrent.newUnmodifiableList();
        }

    }

    /**
     * Recording stand-in for {@link WritableRemoteJsonSource} that captures
     * every {@code buffer} call into a thread-safe list so the test can
     * observe drain activity without racing.
     */
    private static final class RecordingSource<T extends JpaModel> extends WritableRemoteJsonSource<T> {

        final List<BufferedMutation<T>> bufferedMutations = new CopyOnWriteArrayList<>();

        RecordingSource(@NotNull Class<T> modelClass) {
            super(
                new NoopDelegate<>(),
                new ThrowingContract(),
                new EmptyIndexProvider(),
                MinecraftApi.getGson(),
                "recording-" + modelClass.getSimpleName(),
                modelClass,
                3
            );
        }

        @Override
        @SuppressWarnings("unchecked")
        public void buffer(@NotNull BufferedMutation mutation) throws JpaException {
            this.bufferedMutations.add(mutation);
        }

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
