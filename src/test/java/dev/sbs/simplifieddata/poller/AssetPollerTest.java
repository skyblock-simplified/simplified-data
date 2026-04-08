package dev.sbs.simplifieddata.poller;

import com.google.gson.Gson;
import dev.sbs.minecraftapi.MinecraftApi;
import dev.sbs.simplifieddata.client.ETagContext;
import dev.sbs.simplifieddata.client.SkyBlockDataContract;
import dev.sbs.simplifieddata.client.exception.SkyBlockDataException;
import dev.sbs.simplifieddata.client.response.GitHubCommit;
import dev.simplified.client.exception.NotModifiedException;
import dev.simplified.client.response.HttpStatus;
import dev.simplified.client.response.NetworkDetails;
import dev.simplified.client.response.Response;
import dev.simplified.client.request.HttpMethod;
import dev.simplified.client.request.Request;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.persistence.JpaConfig;
import dev.simplified.persistence.JpaSession;
import dev.simplified.persistence.RepositoryFactory;
import dev.simplified.persistence.SessionManager;
import dev.simplified.persistence.asset.ExternalAssetEntryState;
import dev.simplified.persistence.asset.ExternalAssetState;
import dev.simplified.persistence.driver.H2MemoryDriver;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Integration test for {@link AssetPoller} driving a hand-rolled {@link SkyBlockDataContract}
 * stub through a {@link LastResponseAccessor} bridge and a real in-process {@link JpaSession}
 * scoped to the {@code dev.simplified.persistence.asset} package.
 *
 * <p>No Spring context: the poller is instantiated directly with the asset session, the
 * stub contract, the synthetic last-response accessor, and the configuration values. This
 * keeps the integration narrow - the unit under test is the {@code doPoll()} cycle, not
 * Spring's bean wiring.
 *
 * <p>Coverage matrix:
 * <ul>
 *   <li>First poll against empty state populates {@code ExternalAssetState} + one
 *       {@code ExternalAssetEntryState} row per manifest file.</li>
 *   <li>Second poll with the same commit SHA records a no-change cycle.</li>
 *   <li>Second poll with a fresh commit SHA fetches the manifest, computes the diff, and
 *       updates the state tables accordingly.</li>
 *   <li>304 short-circuit path bumps {@code lastCheckedAt} only and skips the manifest fetch.</li>
 *   <li>{@link SkyBlockDataException} during the commits call is swallowed without crashing
 *       the poller.</li>
 *   <li>{@code pollEnabled=false} via constructor flag bypasses the scheduled and startup
 *       entry points entirely.</li>
 * </ul>
 */
@Tag("slow")
class AssetPollerTest {

    private static final @NotNull Gson GSON = MinecraftApi.getGson();

    private SessionManager sessionManager;
    private JpaSession assetSession;
    private StubContract contract;
    private StubLastResponseAccessor accessor;

    @BeforeEach
    void setUp() {
        this.sessionManager = new SessionManager();
        JpaConfig config = JpaConfig.common(new H2MemoryDriver(), "asset_poller_test")
            .withRepositoryFactory(
                RepositoryFactory.builder()
                    .withPackageOf(ExternalAssetState.class)
                    .build()
            )
            .build();
        this.assetSession = this.sessionManager.connect(config);

        this.contract = new StubContract();
        this.accessor = new StubLastResponseAccessor();

        ETagContext.clear();
    }

    @AfterEach
    void tearDown() {
        if (this.sessionManager != null)
            this.sessionManager.shutdown();
        ETagContext.clear();
    }

    @Test
    @DisplayName("first poll populates ExternalAssetState and one row per manifest entry")
    void firstPollPopulatesState() {
        ConcurrentList<GitHubCommit> commits = parseCommits("sha-one");
        this.contract.commitList = commits;
        this.contract.fileContents.put("data/v1/index.json", sampleManifest("sha-one", "aaa", "bbb"));
        this.accessor.next = response(200, Map.of("etag", List.of("W/\"etag-one\"")), commits);

        AssetPoller poller = newPoller(true);
        poller.onApplicationReady();

        this.assetSession.with(session -> {
            ExternalAssetState state = session.find(ExternalAssetState.class, "skyblock-data");
            assertThat(state, is(notNullValue()));
            assertThat(state.getCommitSha().orElse(null), equalTo("sha-one"));
            assertThat(state.getEtag().orElse(null), equalTo("W/\"etag-one\""));
            assertThat(state.getContentSha256().orElse(null), is(notNullValue()));
            assertThat(state.getLastSuccessAt().isPresent(), is(true));

            long count = (long) session.createQuery(
                "SELECT count(e) FROM ExternalAssetEntryState e WHERE e.sourceId = :sid",
                Long.class
            ).setParameter("sid", "skyblock-data").getSingleResult();
            assertThat(count, greaterThan(0L));
        });
    }

    @Test
    @DisplayName("second poll with unchanged commit sha records a no-change cycle")
    void noChangePoll() {
        ConcurrentList<GitHubCommit> commits = parseCommits("sha-one");
        this.contract.commitList = commits;
        this.contract.fileContents.put("data/v1/index.json", sampleManifest("sha-one", "aaa", "bbb"));
        this.accessor.next = response(200, Map.of("etag", List.of("W/\"etag-one\"")), commits);

        AssetPoller poller = newPoller(true);
        poller.onApplicationReady();

        // Second cycle: same commit list and ETag, scheduled cycle should observe equality
        // and short-circuit through recordNoChange().
        this.accessor.next = response(200, Map.of("etag", List.of("W/\"etag-one\"")), commits);
        poller.scheduledPoll();

        this.assetSession.with(session -> {
            ExternalAssetState state = session.find(ExternalAssetState.class, "skyblock-data");
            assertThat(state, is(notNullValue()));
            assertThat(state.getCommitSha().orElse(null), equalTo("sha-one"));
            assertThat(state.getLastCheckedAt(), is(notNullValue()));
        });
    }

    @Test
    @DisplayName("second poll with fresh commit sha triggers diff and state update")
    void changePoll() {
        ConcurrentList<GitHubCommit> firstCommits = parseCommits("sha-one");
        this.contract.commitList = firstCommits;
        this.contract.fileContents.put("data/v1/index.json", sampleManifest("sha-one", "aaa", "bbb"));
        this.accessor.next = response(200, Map.of("etag", List.of("W/\"etag-one\"")), firstCommits);

        AssetPoller poller = newPoller(true);
        poller.onApplicationReady();

        // Second cycle: a fresh commit sha and a new manifest with one changed entry hash.
        ConcurrentList<GitHubCommit> secondCommits = parseCommits("sha-two");
        this.contract.commitList = secondCommits;
        this.contract.fileContents.put("data/v1/index.json", sampleManifest("sha-two", "ccc", "bbb"));
        this.accessor.next = response(200, Map.of("etag", List.of("W/\"etag-two\"")), secondCommits);

        poller.scheduledPoll();

        this.assetSession.with(session -> {
            ExternalAssetState state = session.find(ExternalAssetState.class, "skyblock-data");
            assertThat(state, is(notNullValue()));
            assertThat(state.getCommitSha().orElse(null), equalTo("sha-two"));
            assertThat(state.getEtag().orElse(null), equalTo("W/\"etag-two\""));

            ExternalAssetEntryState items = session.find(
                ExternalAssetEntryState.class,
                new ExternalAssetEntryState.PK("skyblock-data", "data/v1/items/items.json")
            );
            assertThat(items, is(notNullValue()));
            assertThat(items.getEntrySha256(), equalTo("ccc"));
        });
    }

    @Test
    @DisplayName("304 short-circuit bumps lastCheckedAt and skips manifest fetch")
    void shortCircuit304() {
        ConcurrentList<GitHubCommit> commits = parseCommits("sha-one");
        this.contract.commitList = commits;
        this.contract.fileContents.put("data/v1/index.json", sampleManifest("sha-one", "aaa", "bbb"));
        this.accessor.next = response(200, Map.of("etag", List.of("W/\"etag-one\"")), commits);

        AssetPoller poller = newPoller(true);
        poller.onApplicationReady();

        // Second cycle: simulate the framework's 304 short-circuit by having the stub throw
        // a NotModifiedException directly. The framework's InternalErrorDecoder produces this
        // exception type when GitHub returns 304 to a conditional request, and AssetPoller's
        // probeLatestCommit catches it as the "no change" signal.
        this.contract.commitException = buildNotModified();
        this.contract.commitList = null;
        poller.scheduledPoll();

        this.assetSession.with(session -> {
            ExternalAssetState state = session.find(ExternalAssetState.class, "skyblock-data");
            assertThat(state, is(notNullValue()));
            // Commit sha unchanged because the 304 short-circuits before any state mutation
            // beyond the lastCheckedAt bump.
            assertThat(state.getCommitSha().orElse(null), equalTo("sha-one"));
            assertThat(state.getEtag().orElse(null), equalTo("W/\"etag-one\""));
        });
    }

    @Test
    @DisplayName("SkyBlockDataException is swallowed and the poll cycle returns normally")
    void gitHubFailureIsSwallowed() {
        this.contract.commitException = buildFailure(500, "Internal Server Error");

        AssetPoller poller = newPoller(true);
        poller.onApplicationReady();

        this.assetSession.with(session -> {
            ExternalAssetState state = session.find(ExternalAssetState.class, "skyblock-data");
            assertThat(state, is(nullValue()));
        });
    }

    @Test
    @DisplayName("pollEnabled=false short-circuits both startup and scheduled entry points")
    void pollDisabledSkipsBothEntryPoints() {
        ConcurrentList<GitHubCommit> commits = parseCommits("sha-one");
        this.contract.commitList = commits;
        this.contract.fileContents.put("data/v1/index.json", sampleManifest("sha-one", "aaa", "bbb"));
        this.accessor.next = response(200, Map.of("etag", List.of("W/\"etag-one\"")), commits);

        AssetPoller poller = newPoller(false);
        poller.onApplicationReady();
        poller.scheduledPoll();

        this.assetSession.with(session -> {
            ExternalAssetState state = session.find(ExternalAssetState.class, "skyblock-data");
            assertThat(state, is(nullValue()));
        });
    }

    // --- helpers --- //

    private @NotNull AssetPoller newPoller(boolean pollEnabled) {
        return new AssetPoller(this.assetSession, this.contract, this.accessor, "skyblock-data", pollEnabled);
    }

    private static @NotNull String sampleManifest(@NotNull String commitSha, @NotNull String itemsSha, @NotNull String mobsSha) {
        return """
            {
              "version": 1,
              "generated_at": "2026-04-07T00:00:00Z",
              "commit_sha": "%s",
              "count": 2,
              "files": [
                {
                  "path": "data/v1/items/items.json",
                  "category": "items",
                  "table_name": "item",
                  "model_class": "dev.sbs.minecraftapi.persistence.model.Item",
                  "content_sha256": "%s",
                  "bytes": 10,
                  "has_extra": false
                },
                {
                  "path": "data/v1/mobs/mobs.json",
                  "category": "mobs",
                  "table_name": "mob",
                  "model_class": "dev.sbs.minecraftapi.persistence.model.MobType",
                  "content_sha256": "%s",
                  "bytes": 20,
                  "has_extra": false
                }
              ]
            }
            """.formatted(commitSha, itemsSha, mobsSha);
    }

    private static @NotNull ConcurrentList<GitHubCommit> parseCommits(@NotNull String sha) {
        String json = """
            [
              {
                "sha": "%s",
                "commit": {
                  "message": "test commit",
                  "committer": {
                    "name": "Tester",
                    "date": "2026-04-07T00:00:00Z"
                  }
                }
              }
            ]
            """.formatted(sha);
        // Build through Gson into a single-element ConcurrentList<GitHubCommit>.
        GitHubCommit[] array = GSON.fromJson(json, GitHubCommit[].class);
        ConcurrentList<GitHubCommit> list = Concurrent.newList();
        for (GitHubCommit commit : array)
            list.add(commit);
        return list;
    }

    private static @NotNull Response<?> response(int status, @NotNull Map<String, List<String>> headers, @NotNull Object body) {
        ConcurrentMap<String, ConcurrentList<String>> wrappedHeaders = Concurrent.newMap();
        headers.forEach((key, values) -> wrappedHeaders.put(key, Concurrent.newList(values)));

        Map<String, Collection<String>> feignHeaders = new HashMap<>();
        headers.forEach(feignHeaders::put);
        feign.Request feignRequest = feign.Request.create(
            feign.Request.HttpMethod.GET,
            "https://api.github.com/repos/skyblock-simplified/skyblock-data/commits?sha=master",
            Map.of(),
            feign.Request.Body.empty(),
            new feign.RequestTemplate()
        );
        feign.Response feignResponse = feign.Response.builder()
            .status(status)
            .reason("test")
            .request(feignRequest)
            .headers(feignHeaders)
            .body("", StandardCharsets.UTF_8)
            .build();

        NetworkDetails details = new NetworkDetails(feignResponse);
        Request request = new Request.Impl(HttpMethod.GET, feignRequest.url());
        HttpStatus httpStatus = HttpStatus.of(status);
        return new Response.Impl<>(body, details, httpStatus, request, wrappedHeaders);
    }

    private static @NotNull SkyBlockDataException buildFailure(int status, @NotNull String reason) {
        feign.Request feignRequest = feign.Request.create(
            feign.Request.HttpMethod.GET,
            "https://api.github.com/repos/skyblock-simplified/skyblock-data/commits?sha=master",
            Map.of(),
            feign.Request.Body.empty(),
            new feign.RequestTemplate()
        );
        feign.Response feignResponse = feign.Response.builder()
            .status(status)
            .reason(reason)
            .request(feignRequest)
            .headers(Map.of())
            .body("{\"message\":\"" + reason + "\"}", StandardCharsets.UTF_8)
            .build();
        return new SkyBlockDataException("getLatestMasterCommit", feignResponse);
    }

    private static @NotNull NotModifiedException buildNotModified() {
        feign.Request feignRequest = feign.Request.create(
            feign.Request.HttpMethod.GET,
            "https://api.github.com/repos/skyblock-simplified/skyblock-data/commits?sha=master",
            Map.of(),
            feign.Request.Body.empty(),
            new feign.RequestTemplate()
        );
        feign.Response feignResponse = feign.Response.builder()
            .status(304)
            .reason("Not Modified")
            .request(feignRequest)
            .headers(Map.of())
            .body(new byte[0])
            .build();
        return new NotModifiedException("getLatestMasterCommit", feignResponse);
    }

    /** Hand-rolled contract stub. */
    private static final class StubContract implements SkyBlockDataContract {

        private final @NotNull Map<String, String> fileContents = new HashMap<>();
        private @Nullable ConcurrentList<GitHubCommit> commitList;
        private @Nullable RuntimeException commitException;

        @Override
        public @NotNull ConcurrentList<GitHubCommit> getLatestMasterCommit() throws SkyBlockDataException {
            if (this.commitException != null)
                throw this.commitException;

            if (this.commitList == null)
                throw new IllegalStateException("StubContract.commitList not set - test should not call getLatestMasterCommit on this path");

            return this.commitList;
        }

        @Override
        public byte @NotNull [] getFileContent(@NotNull String path) throws SkyBlockDataException {
            String body = this.fileContents.get(path);

            if (body == null)
                throw new IllegalStateException("StubContract.fileContents missing entry for path '" + path + "'");

            return body.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }

    }

    /** Hand-rolled accessor stub - returns a single canned response per call. */
    private static final class StubLastResponseAccessor implements LastResponseAccessor {

        private @Nullable Response<?> next;

        @Override
        public @NotNull Optional<Response<?>> getLastResponse() {
            return Optional.ofNullable(this.next);
        }

    }

}
