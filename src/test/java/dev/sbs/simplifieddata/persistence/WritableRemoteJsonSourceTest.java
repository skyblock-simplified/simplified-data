package dev.sbs.simplifieddata.persistence;

import com.google.gson.Gson;
import dev.sbs.minecraftapi.MinecraftApi;
import dev.sbs.minecraftapi.persistence.model.ZodiacEvent;
import dev.sbs.simplifieddata.client.SkyBlockDataWriteContract;
import dev.sbs.simplifieddata.client.exception.SkyBlockDataException;
import dev.sbs.simplifieddata.client.request.PutContentRequest;
import dev.sbs.simplifieddata.client.response.GitHubContentEnvelope;
import dev.sbs.simplifieddata.client.response.GitHubPutResponse;
import dev.sbs.simplifieddata.write.WriteMetrics;
import dev.simplified.client.exception.PreconditionFailedException;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.JpaRepository;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.source.FileFetcher;
import dev.simplified.persistence.source.IndexProvider;
import dev.simplified.persistence.source.ManifestIndex;
import dev.simplified.persistence.source.Source;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link WritableRemoteJsonSource} driven against a hand-rolled
 * {@link SkyBlockDataWriteContract} stub and a canned
 * {@link ManifestIndex} fixture.
 *
 * <p>No Spring context, no live Hazelcast, no {@link JpaRepository} - every
 * test calls {@code upsert}, {@code delete}, and {@code commitBatch} directly
 * on the source. The {@link #delegate} is a no-op lambda since these tests
 * only exercise the write path; the {@code load(JpaRepository)} method is
 * covered separately via a single delegation test.
 *
 * <p>Coverage matrix:
 * <ul>
 *   <li>{@code commitBatch} on an empty buffer returns {@code CommitBatchResult.empty()}
 *       and does NOT touch the contract.</li>
 *   <li>{@code upsert} of a single new entity appends it to the target file
 *       and issues one PUT with the decoded JSON.</li>
 *   <li>{@code upsert} of an existing entity replaces it in place and
 *       preserves the order of the other entries.</li>
 *   <li>{@code delete} removes the entity from the list and PUTs the
 *       shortened JSON.</li>
 *   <li>412 Precondition Failed retries up to the configured cap with fresh
 *       blob SHA fetches.</li>
 *   <li>Exhausting 412 retries escalates the failures back to the caller.</li>
 *   <li>A non-412 {@link SkyBlockDataException} from {@code putFileContent}
 *       escalates all mutations to the caller.</li>
 *   <li>{@code load} delegates verbatim to the injected source.</li>
 * </ul>
 */
class WritableRemoteJsonSourceTest {

    private static final @NotNull Gson GSON = MinecraftApi.getGson();
    private static final @NotNull String SOURCE_ID = "skyblock-data";
    private static final @NotNull String FILE_PATH = "data/v1/world/zodiac_events.json";
    private static final @NotNull String INITIAL_BLOB_SHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    private static final @NotNull String NEW_BLOB_SHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    private static final @NotNull String NEW_COMMIT_SHA = "cccccccccccccccccccccccccccccccccccccccc";

    private RecordingDelegate delegate;
    private StubIndexProvider indexProvider;
    private StubWriteContract contract;
    private StubFileFetcher fileFetcher;

    @BeforeEach
    void setUp() {
        this.delegate = new RecordingDelegate();
        this.indexProvider = new StubIndexProvider();
        this.contract = new StubWriteContract();
        this.fileFetcher = new StubFileFetcher();
    }

    @Test
    @DisplayName("commitBatch on an empty buffer returns empty() and does not touch the contract")
    void commitBatchEmpty() {
        WritableRemoteJsonSource<ZodiacEvent> source = newSource(initialFile(List.of()));

        WritableRemoteJsonSource.CommitBatchResult result = source.commitBatch();

        assertThat(result.isEmpty(), is(true));
        assertThat(result.isSuccess(), is(true));
        assertThat(this.contract.getMetadataCallCount(), equalTo(0));
        assertThat(this.contract.getPutCallCount(), equalTo(0));
    }

    @Test
    @DisplayName("upsert of a new entity issues one PUT with the appended JSON and returns success")
    void upsertNewEntityAppends() throws Exception {
        ZodiacEvent existing = event("YEAR_OF_THE_SEAL", "Year of the Seal", 414);
        WritableRemoteJsonSource<ZodiacEvent> source = newSource(initialFile(List.of(existing)));

        ZodiacEvent fresh = event("YEAR_OF_THE_DOLPHIN", "Year of the Dolphin", 415);
        source.upsert(fresh);

        WritableRemoteJsonSource.CommitBatchResult result = source.commitBatch();

        assertThat(result.isSuccess(), is(true));
        assertThat(result.getAppliedCount(), equalTo(1));
        assertThat(result.getCommitSha(), equalTo(NEW_COMMIT_SHA));
        assertThat(this.contract.getPutCallCount(), equalTo(1));

        PutContentRequest body = this.contract.getLastPutBody();
        assertThat(body.getSha(), equalTo(INITIAL_BLOB_SHA));
        assertThat(body.getMessage(), containsString("Update ZodiacEvent: 1 mutation"));
        assertThat(body.getBranch(), equalTo("master"));

        ConcurrentList<ZodiacEvent> written = decodeBody(body.getContent());
        assertThat(written, hasSize(2));
        assertThat(written.get(0).getId(), equalTo("YEAR_OF_THE_SEAL"));
        assertThat(written.get(1).getId(), equalTo("YEAR_OF_THE_DOLPHIN"));
    }

    @Test
    @DisplayName("upsert of an existing entity replaces it in place")
    void upsertReplacesExisting() throws Exception {
        ZodiacEvent a = event("YEAR_OF_THE_SEAL", "Year of the Seal", 414);
        ZodiacEvent b = event("YEAR_OF_THE_WHALE", "Year of the Whale", 413);
        WritableRemoteJsonSource<ZodiacEvent> source = newSource(initialFile(List.of(a, b)));

        ZodiacEvent updated = event("YEAR_OF_THE_SEAL", "Year of the Seal (updated)", 415);
        source.upsert(updated);

        WritableRemoteJsonSource.CommitBatchResult result = source.commitBatch();

        assertThat(result.isSuccess(), is(true));
        ConcurrentList<ZodiacEvent> written = decodeBody(this.contract.getLastPutBody().getContent());
        assertThat(written, hasSize(2));
        assertThat(written.get(0).getId(), equalTo("YEAR_OF_THE_SEAL"));
        assertThat(written.get(0).getName(), equalTo("Year of the Seal (updated)"));
        assertThat(written.get(0).getReleaseYear(), equalTo(415));
        assertThat(written.get(1).getId(), equalTo("YEAR_OF_THE_WHALE"));
    }

    @Test
    @DisplayName("delete removes the matching entity and PUTs the shortened list")
    void deleteRemovesEntity() throws Exception {
        ZodiacEvent a = event("YEAR_OF_THE_SEAL", "Year of the Seal", 414);
        ZodiacEvent b = event("YEAR_OF_THE_WHALE", "Year of the Whale", 413);
        WritableRemoteJsonSource<ZodiacEvent> source = newSource(initialFile(List.of(a, b)));

        source.delete(a);

        WritableRemoteJsonSource.CommitBatchResult result = source.commitBatch();

        assertThat(result.isSuccess(), is(true));
        ConcurrentList<ZodiacEvent> written = decodeBody(this.contract.getLastPutBody().getContent());
        assertThat(written, hasSize(1));
        assertThat(written.get(0).getId(), equalTo("YEAR_OF_THE_WHALE"));
    }

    @Test
    @DisplayName("412 Precondition Failed retries with a fresh blob SHA up to the configured cap")
    void preconditionRetry() throws Exception {
        ZodiacEvent existing = event("YEAR_OF_THE_SEAL", "Year of the Seal", 414);
        this.contract.queueInitialFile(encodeBody(List.of(existing)), INITIAL_BLOB_SHA);
        this.contract.queueInitialFile(encodeBody(List.of(existing)), NEW_BLOB_SHA);
        // First put returns 412, second put succeeds.
        this.contract.queuePutBehavior(StubWriteContract.PutOutcome.PRECONDITION_FAILED);
        this.contract.queuePutBehavior(StubWriteContract.PutOutcome.SUCCESS);

        WritableRemoteJsonSource<ZodiacEvent> source = newSource(/*queueUnusedInitial=*/ false);
        source.upsert(event("YEAR_OF_THE_DOLPHIN", "Year of the Dolphin", 415));

        WritableRemoteJsonSource.CommitBatchResult result = source.commitBatch();

        assertThat(result.isSuccess(), is(true));
        assertThat(this.contract.getMetadataCallCount(), equalTo(2));
        assertThat(this.contract.getPutCallCount(), equalTo(2));
        // The second PUT must carry the refreshed blob SHA.
        assertThat(this.contract.getLastPutBody().getSha(), equalTo(NEW_BLOB_SHA));
    }

    @Test
    @DisplayName("Exhausting 412 retries escalates the buffered mutations back to the caller")
    void preconditionExhaustedEscalates() throws Exception {
        ZodiacEvent existing = event("YEAR_OF_THE_SEAL", "Year of the Seal", 414);
        // Queue 4 metadata responses (initial + 3 retries), all PUTs return 412.
        for (int i = 0; i < 4; i++)
            this.contract.queueInitialFile(encodeBody(List.of(existing)), INITIAL_BLOB_SHA + i);

        for (int i = 0; i < 4; i++)
            this.contract.queuePutBehavior(StubWriteContract.PutOutcome.PRECONDITION_FAILED);

        WritableRemoteJsonSource<ZodiacEvent> source = newSource(/*queueUnusedInitial=*/ false);
        source.upsert(event("YEAR_OF_THE_DOLPHIN", "Year of the Dolphin", 415));

        WritableRemoteJsonSource.CommitBatchResult result = source.commitBatch();

        assertThat(result.isSuccess(), is(false));
        assertThat(result.isEmpty(), is(false));
        assertThat(result.getFailures(), hasSize(1));
        assertThat(result.getFailureCause(), notNullValue());
        // Initial attempt + 3 retries = 4 PUT calls.
        assertThat(this.contract.getPutCallCount(), equalTo(4));
    }

    @Test
    @DisplayName("Non-412 SkyBlockDataException from PUT escalates mutations without retrying")
    void nonPreconditionFailureEscalates() throws Exception {
        ZodiacEvent existing = event("YEAR_OF_THE_SEAL", "Year of the Seal", 414);
        this.contract.queueInitialFile(encodeBody(List.of(existing)), INITIAL_BLOB_SHA);
        this.contract.queuePutBehavior(StubWriteContract.PutOutcome.GENERIC_ERROR);

        WritableRemoteJsonSource<ZodiacEvent> source = newSource(/*queueUnusedInitial=*/ false);
        source.upsert(event("YEAR_OF_THE_DOLPHIN", "Year of the Dolphin", 415));

        WritableRemoteJsonSource.CommitBatchResult result = source.commitBatch();

        assertThat(result.isSuccess(), is(false));
        assertThat(result.getFailures(), hasSize(1));
        assertThat(this.contract.getPutCallCount(), equalTo(1));
    }

    @Test
    @DisplayName("load() delegates verbatim to the injected delegate source")
    void loadDelegates() throws Exception {
        ZodiacEvent event = event("YEAR_OF_THE_SEAL", "Year of the Seal", 414);
        this.delegate.stubResult = Concurrent.newList(event);

        WritableRemoteJsonSource<ZodiacEvent> source = newSource(initialFile(List.of()));

        ConcurrentList<ZodiacEvent> result = source.load(null);

        assertThat(result, hasSize(1));
        assertThat(result.get(0).getId(), equalTo("YEAR_OF_THE_SEAL"));
        assertThat(this.delegate.callCount, equalTo(1));
    }

    @Test
    @DisplayName("upsert throws JpaException when the entity has a null @Id value")
    void upsertNullIdThrows() {
        WritableRemoteJsonSource<ZodiacEvent> source = newSource(initialFile(List.of()));

        ZodiacEvent bad = new ZodiacEvent();
        // Clear the default empty-string id via reflection so the accessor returns null.
        setField(bad, "id", null);

        assertThrows(JpaException.class, () -> source.upsert(bad));
    }

    // --- Phase 6b.1 stageBatch() tests --- //

    @Test
    @DisplayName("stageBatch on an empty buffer returns StagedBatch.empty()")
    void stageBatchEmpty() {
        WritableRemoteJsonSource<ZodiacEvent> source = newStagingSource();

        dev.sbs.simplifieddata.write.StagedBatch<ZodiacEvent> staged = source.stageBatch();

        assertThat(staged.isEmpty(), is(true));
        assertThat(staged.getFileSnapshots().size(), equalTo(0));
    }

    @Test
    @DisplayName("stageBatch on a single-file source produces one file snapshot with appended upsert")
    void stageBatchSingleFileAppend() throws Exception {
        ZodiacEvent existing = event("YEAR_OF_THE_SEAL", "Year of the Seal", 414);
        this.fileFetcher.put(FILE_PATH, GSON.toJson(List.of(existing)));

        WritableRemoteJsonSource<ZodiacEvent> source = newStagingSource();
        source.upsert(event("YEAR_OF_THE_DOLPHIN", "Year of the Dolphin", 415));

        dev.sbs.simplifieddata.write.StagedBatch<ZodiacEvent> staged = source.stageBatch();

        assertThat(staged.isEmpty(), is(false));
        assertThat(staged.getFileSnapshots().size(), equalTo(1));
        assertThat(staged.getMutationCount(), equalTo(1));

        ConcurrentList<ZodiacEvent> mutatedPrimary = staged.getFileSnapshots().get(FILE_PATH);
        assertThat(mutatedPrimary, hasSize(2));
        assertThat(mutatedPrimary.get(0).getId(), equalTo("YEAR_OF_THE_SEAL"));
        assertThat(mutatedPrimary.get(1).getId(), equalTo("YEAR_OF_THE_DOLPHIN"));
    }

    @Test
    @DisplayName("stageBatch routes an upsert to the extras file when the id currently lives there")
    void stageBatchRoutesUpsertToExtras() throws Exception {
        this.indexProvider.hasExtra = true;

        ZodiacEvent inPrimary = event("YEAR_OF_THE_SEAL", "Year of the Seal", 414);
        ZodiacEvent inExtras = event("YEAR_OF_THE_WHALE", "Year of the Whale (corrected)", 413);
        this.fileFetcher.put(FILE_PATH, GSON.toJson(List.of(inPrimary)));
        this.fileFetcher.put(this.indexProvider.extraPath, GSON.toJson(List.of(inExtras)));

        WritableRemoteJsonSource<ZodiacEvent> source = newStagingSource();
        // Upsert the whale with a new year - it should stay in the extras file.
        source.upsert(event("YEAR_OF_THE_WHALE", "Year of the Whale (corrected)", 499));

        dev.sbs.simplifieddata.write.StagedBatch<ZodiacEvent> staged = source.stageBatch();

        assertThat(staged.getFileSnapshots().size(), equalTo(1));
        assertThat(staged.getFileSnapshots().containsKey(this.indexProvider.extraPath), is(true));
        assertThat(staged.getFileSnapshots().containsKey(FILE_PATH), is(false));

        ConcurrentList<ZodiacEvent> mutatedExtra = staged.getFileSnapshots().get(this.indexProvider.extraPath);
        assertThat(mutatedExtra, hasSize(1));
        assertThat(mutatedExtra.get(0).getReleaseYear(), equalTo(499));
    }

    @Test
    @DisplayName("stageBatch routes a new-id upsert to the primary file when extras exists but lacks the id")
    void stageBatchRoutesNewIdToPrimary() throws Exception {
        this.indexProvider.hasExtra = true;

        ZodiacEvent inPrimary = event("YEAR_OF_THE_SEAL", "Year of the Seal", 414);
        ZodiacEvent inExtras = event("YEAR_OF_THE_WHALE", "Year of the Whale", 413);
        this.fileFetcher.put(FILE_PATH, GSON.toJson(List.of(inPrimary)));
        this.fileFetcher.put(this.indexProvider.extraPath, GSON.toJson(List.of(inExtras)));

        WritableRemoteJsonSource<ZodiacEvent> source = newStagingSource();
        // Brand-new id - should land in the primary file by default.
        source.upsert(event("YEAR_OF_THE_DOLPHIN", "Year of the Dolphin", 415));

        dev.sbs.simplifieddata.write.StagedBatch<ZodiacEvent> staged = source.stageBatch();

        assertThat(staged.getFileSnapshots().size(), equalTo(1));
        assertThat(staged.getFileSnapshots().containsKey(FILE_PATH), is(true));
        assertThat(staged.getFileSnapshots().containsKey(this.indexProvider.extraPath), is(false));

        ConcurrentList<ZodiacEvent> mutatedPrimary = staged.getFileSnapshots().get(FILE_PATH);
        assertThat(mutatedPrimary, hasSize(2));
        assertThat(mutatedPrimary.get(1).getId(), equalTo("YEAR_OF_THE_DOLPHIN"));
    }

    @Test
    @DisplayName("stageBatch routes a delete to whichever file currently owns the id")
    void stageBatchRoutesDeleteToOwningFile() throws Exception {
        this.indexProvider.hasExtra = true;

        ZodiacEvent inPrimary = event("YEAR_OF_THE_SEAL", "Year of the Seal", 414);
        ZodiacEvent inExtras = event("YEAR_OF_THE_WHALE", "Year of the Whale", 413);
        this.fileFetcher.put(FILE_PATH, GSON.toJson(List.of(inPrimary)));
        this.fileFetcher.put(this.indexProvider.extraPath, GSON.toJson(List.of(inExtras)));

        WritableRemoteJsonSource<ZodiacEvent> source = newStagingSource();
        source.delete(event("YEAR_OF_THE_WHALE", "Year of the Whale", 413));

        dev.sbs.simplifieddata.write.StagedBatch<ZodiacEvent> staged = source.stageBatch();

        assertThat(staged.getFileSnapshots().size(), equalTo(1));
        assertThat(staged.getFileSnapshots().containsKey(this.indexProvider.extraPath), is(true));

        ConcurrentList<ZodiacEvent> mutatedExtra = staged.getFileSnapshots().get(this.indexProvider.extraPath);
        assertThat(mutatedExtra, hasSize(0));
    }

    @Test
    @DisplayName("stageBatch UPSERT of an id present in BOTH files updates ONLY the extras copy (extras wins)")
    void stageBatchUpsertConflictKeepsExtrasAuthoritative() throws Exception {
        this.indexProvider.hasExtra = true;

        // Same id in both files with different state - extras is the correction.
        ZodiacEvent upstream = event("YEAR_OF_THE_SEAL", "Year of the Seal", 414);
        ZodiacEvent correction = event("YEAR_OF_THE_SEAL", "Year of the Seal (corrected)", 499);
        this.fileFetcher.put(FILE_PATH, GSON.toJson(List.of(upstream)));
        this.fileFetcher.put(this.indexProvider.extraPath, GSON.toJson(List.of(correction)));

        WritableRemoteJsonSource<ZodiacEvent> source = newStagingSource();
        source.upsert(event("YEAR_OF_THE_SEAL", "Year of the Seal (re-corrected)", 500));

        dev.sbs.simplifieddata.write.StagedBatch<ZodiacEvent> staged = source.stageBatch();

        // Only the extras file should be dirty - primary is untouched.
        assertThat(staged.getFileSnapshots().size(), equalTo(1));
        assertThat(staged.getFileSnapshots().containsKey(this.indexProvider.extraPath), is(true));
        assertThat(staged.getFileSnapshots().containsKey(FILE_PATH), is(false));

        ConcurrentList<ZodiacEvent> mutatedExtra = staged.getFileSnapshots().get(this.indexProvider.extraPath);
        assertThat(mutatedExtra, hasSize(1));
        assertThat(mutatedExtra.get(0).getName(), equalTo("Year of the Seal (re-corrected)"));
        assertThat(mutatedExtra.get(0).getReleaseYear(), equalTo(500));
    }

    @Test
    @DisplayName("stageBatch DELETE of an id present in BOTH files removes from BOTH files")
    void stageBatchDeleteConflictRemovesFromBothFiles() throws Exception {
        this.indexProvider.hasExtra = true;

        ZodiacEvent upstream = event("YEAR_OF_THE_SEAL", "Year of the Seal", 414);
        ZodiacEvent correction = event("YEAR_OF_THE_SEAL", "Year of the Seal (corrected)", 499);
        ZodiacEvent otherPrimary = event("YEAR_OF_THE_WHALE", "Year of the Whale", 413);
        this.fileFetcher.put(FILE_PATH, GSON.toJson(List.of(upstream, otherPrimary)));
        this.fileFetcher.put(this.indexProvider.extraPath, GSON.toJson(List.of(correction)));

        WritableRemoteJsonSource<ZodiacEvent> source = newStagingSource();
        source.delete(correction);

        dev.sbs.simplifieddata.write.StagedBatch<ZodiacEvent> staged = source.stageBatch();

        // BOTH files should be dirty - the stale primary copy was removed.
        assertThat(staged.getFileSnapshots().size(), equalTo(2));
        assertThat(staged.getFileSnapshots().containsKey(FILE_PATH), is(true));
        assertThat(staged.getFileSnapshots().containsKey(this.indexProvider.extraPath), is(true));

        ConcurrentList<ZodiacEvent> mutatedPrimary = staged.getFileSnapshots().get(FILE_PATH);
        ConcurrentList<ZodiacEvent> mutatedExtra = staged.getFileSnapshots().get(this.indexProvider.extraPath);
        assertThat(mutatedPrimary, hasSize(1));
        assertThat(mutatedPrimary.get(0).getId(), equalTo("YEAR_OF_THE_WHALE"));
        assertThat(mutatedExtra, hasSize(0));
    }

    @Test
    @DisplayName("stageBatch produces no dirty files when every upsert is byte-identical to current state")
    void stageBatchSuppressesNoOp() throws Exception {
        ZodiacEvent existing = event("YEAR_OF_THE_SEAL", "Year of the Seal", 414);
        this.fileFetcher.put(FILE_PATH, GSON.toJson(List.of(existing)));

        WritableRemoteJsonSource<ZodiacEvent> source = newStagingSource();
        // Upsert the identical entity - post-mutation serialization should match.
        source.upsert(event("YEAR_OF_THE_SEAL", "Year of the Seal", 414));

        dev.sbs.simplifieddata.write.StagedBatch<ZodiacEvent> staged = source.stageBatch();

        assertThat(staged.isEmpty(), is(true));
    }

    /**
     * Creates a source configured for the stageBatch tests - no write contract
     * calls, fileFetcher pre-populated with per-test file content.
     */
    private @NotNull WritableRemoteJsonSource<ZodiacEvent> newStagingSource() {
        return new WritableRemoteJsonSource<>(
            this.delegate,
            this.contract,
            this.fileFetcher,
            this.indexProvider,
            GSON,
            SOURCE_ID,
            ZodiacEvent.class,
            3,
            new WriteMetrics(new SimpleMeterRegistry())
        );
    }

    // --- helper plumbing below --- //

    private @NotNull WritableRemoteJsonSource<ZodiacEvent> newSource(byte[] initialBody) {
        this.contract.queueInitialFile(initialBody, INITIAL_BLOB_SHA);
        // Default PUT outcome for happy-path tests.
        this.contract.queuePutBehavior(StubWriteContract.PutOutcome.SUCCESS);
        return new WritableRemoteJsonSource<>(
            this.delegate,
            this.contract,
            this.fileFetcher,
            this.indexProvider,
            GSON,
            SOURCE_ID,
            ZodiacEvent.class,
            3,
            new WriteMetrics(new SimpleMeterRegistry())
        );
    }

    private @NotNull WritableRemoteJsonSource<ZodiacEvent> newSource(boolean queueUnusedInitial) {
        if (queueUnusedInitial) {
            this.contract.queueInitialFile(encodeBody(List.of()), INITIAL_BLOB_SHA);
            this.contract.queuePutBehavior(StubWriteContract.PutOutcome.SUCCESS);
        }
        return new WritableRemoteJsonSource<>(
            this.delegate,
            this.contract,
            this.fileFetcher,
            this.indexProvider,
            GSON,
            SOURCE_ID,
            ZodiacEvent.class,
            3,
            new WriteMetrics(new SimpleMeterRegistry())
        );
    }

    private static @NotNull ZodiacEvent event(String id, String name, int releaseYear) {
        ZodiacEvent e = new ZodiacEvent();
        setField(e, "id", id);
        setField(e, "name", name);
        setField(e, "releaseYear", releaseYear);
        return e;
    }

    private static void setField(Object target, String fieldName, Object value) {
        try {
            java.lang.reflect.Field field = ZodiacEvent.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(target, value);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private static byte[] encodeBody(List<ZodiacEvent> events) {
        String json = GSON.toJson(events);
        return json.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] initialFile(List<ZodiacEvent> events) {
        return encodeBody(events);
    }

    private static @NotNull ConcurrentList<ZodiacEvent> decodeBody(String base64Content) {
        byte[] decoded = Base64.getDecoder().decode(base64Content);
        String json = new String(decoded, StandardCharsets.UTF_8);
        ZodiacEvent[] arr = GSON.fromJson(json, ZodiacEvent[].class);
        ConcurrentList<ZodiacEvent> list = Concurrent.newList();
        for (ZodiacEvent e : arr)
            list.add(e);
        return list;
    }

    // --- stubs --- //

    private static final class RecordingDelegate implements Source<ZodiacEvent> {

        @NotNull ConcurrentList<ZodiacEvent> stubResult = Concurrent.newList();
        int callCount = 0;

        @Override
        public @NotNull ConcurrentList<ZodiacEvent> load(@NotNull JpaRepository<ZodiacEvent> repository) {
            this.callCount++;
            return this.stubResult;
        }

    }

    /**
     * In-memory {@link FileFetcher} stub - call sites pre-register file contents
     * by path, and {@code fetchFile} returns the registered content or throws if
     * no registration exists for the requested path.
     */
    private static final class StubFileFetcher implements FileFetcher {

        private final java.util.Map<String, String> files = new java.util.HashMap<>();

        void put(@NotNull String path, @NotNull String content) {
            this.files.put(path, content);
        }

        @Override
        public @NotNull String fetchFile(@NotNull String path) {
            String content = this.files.get(path);

            if (content == null)
                throw new IllegalStateException("StubFileFetcher has no registration for path '" + path + "'");

            return content;
        }

    }

    private static final class StubIndexProvider implements IndexProvider {

        boolean hasExtra = false;
        @NotNull String extraPath = "data/v1/world/zodiac_events_extra.json";

        @Override
        public @NotNull ManifestIndex loadIndex() {
            // Hand-roll the manifest via Gson to avoid relying on private constructors.
            String extraBlock = this.hasExtra
                ? ",\"has_extra\":true,\"extra_path\":\"" + this.extraPath + "\",\"extra_sha256\":\"deadbeef\",\"extra_bytes\":256"
                : ",\"has_extra\":false";
            String json = "{"
                + "\"version\":1,"
                + "\"generated_at\":\"2026-04-09T12:00:00Z\","
                + "\"commit_sha\":null,"
                + "\"count\":1,"
                + "\"files\":[{"
                + "\"path\":\"" + FILE_PATH + "\","
                + "\"category\":\"world\","
                + "\"table_name\":\"zodiac_events\","
                + "\"model_class\":\"" + ZodiacEvent.class.getName() + "\","
                + "\"content_sha256\":\"deadbeef\","
                + "\"bytes\":1024"
                + extraBlock
                + "}]}";
            return GSON.fromJson(json, ManifestIndex.class);
        }

    }

    private static final class StubWriteContract implements SkyBlockDataWriteContract {

        enum PutOutcome { SUCCESS, PRECONDITION_FAILED, GENERIC_ERROR }

        private final List<byte[]> queuedBodies = new ArrayList<>();
        private final List<String> queuedShas = new ArrayList<>();
        private final List<PutOutcome> queuedPuts = new ArrayList<>();
        private final AtomicInteger metadataCallCount = new AtomicInteger();
        private final AtomicInteger putCallCount = new AtomicInteger();
        private PutContentRequest lastPutBody;

        void queueInitialFile(byte[] body, String blobSha) {
            this.queuedBodies.add(body);
            this.queuedShas.add(blobSha);
        }

        void queuePutBehavior(PutOutcome outcome) {
            this.queuedPuts.add(outcome);
        }

        int getMetadataCallCount() {
            return this.metadataCallCount.get();
        }

        int getPutCallCount() {
            return this.putCallCount.get();
        }

        PutContentRequest getLastPutBody() {
            return this.lastPutBody;
        }

        @Override
        public @NotNull GitHubContentEnvelope getFileMetadata(@NotNull String path) throws SkyBlockDataException {
            int idx = this.metadataCallCount.getAndIncrement();

            if (idx >= this.queuedBodies.size())
                throw new IllegalStateException("No queued envelope for metadata call " + idx);

            byte[] body = this.queuedBodies.get(idx);
            String sha = this.queuedShas.get(idx);
            String base64 = Base64.getEncoder().encodeToString(body);
            String json = """
                {
                  "name": "file.json",
                  "path": "%s",
                  "sha": "%s",
                  "size": %d,
                  "content": "%s",
                  "encoding": "base64"
                }
                """.formatted(path, sha, body.length, base64);
            return GSON.fromJson(json, GitHubContentEnvelope.class);
        }

        @Override
        public @NotNull GitHubPutResponse putFileContent(
            @NotNull String path,
            @NotNull PutContentRequest body
        ) throws SkyBlockDataException {
            int idx = this.putCallCount.getAndIncrement();
            this.lastPutBody = body;

            if (idx >= this.queuedPuts.size())
                throw new IllegalStateException("No queued PUT outcome for call " + idx);

            PutOutcome outcome = this.queuedPuts.get(idx);

            return switch (outcome) {
                case SUCCESS -> GSON.fromJson(
                    """
                    {
                      "content": { "sha": "%s" },
                      "commit":  { "sha": "%s" }
                    }
                    """.formatted(NEW_BLOB_SHA, NEW_COMMIT_SHA),
                    GitHubPutResponse.class
                );
                case PRECONDITION_FAILED -> throw new PreconditionFailedException("PUT /fake", fakeResponse(412, "Precondition Failed"));
                case GENERIC_ERROR -> throw new SkyBlockDataException("PUT /fake", fakeResponse(500, "Internal Server Error"));
            };
        }

        private static @NotNull feign.Response fakeResponse(int status, @NotNull String reason) {
            feign.Request request = feign.Request.create(
                feign.Request.HttpMethod.PUT,
                "https://api.github.com/fake",
                java.util.Map.of(),
                feign.Request.Body.empty(),
                new feign.RequestTemplate()
            );
            return feign.Response.builder()
                .status(status)
                .reason(reason)
                .request(request)
                .headers(java.util.Map.of())
                .body("{\"message\":\"" + reason + "\",\"documentation_url\":\"\"}", StandardCharsets.UTF_8)
                .build();
        }

    }

}
