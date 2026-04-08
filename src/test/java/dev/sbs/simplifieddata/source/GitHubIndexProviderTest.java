package dev.sbs.simplifieddata.source;

import com.google.gson.Gson;
import dev.sbs.minecraftapi.MinecraftApi;
import dev.sbs.simplifieddata.client.SkyBlockDataContract;
import dev.sbs.simplifieddata.client.exception.SkyBlockDataException;
import dev.sbs.simplifieddata.client.response.GitHubCommit;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.source.ManifestIndex;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link GitHubIndexProvider}.
 *
 * <p>The Feign contract is stubbed with a plain Java implementation; the Gson instance is
 * reused from {@link MinecraftApi#getGson()} so the production {@code ConcurrentList} type
 * adapter is in play. No network I/O.
 */
class GitHubIndexProviderTest {

    private static final @NotNull String SAMPLE_MANIFEST_JSON = """
        {
          "version": 1,
          "generated_at": "2026-04-07T12:00:00Z",
          "commit_sha": "abc123",
          "count": 2,
          "files": [
            {
              "path": "data/v1/items/items.json",
              "category": "items",
              "table_name": "item",
              "model_class": "dev.sbs.minecraftapi.model.Item",
              "content_sha256": "deadbeef",
              "bytes": 7102271,
              "has_extra": true,
              "extra_path": "data/v1/items/items_extra.json",
              "extra_sha256": "cafebabe",
              "extra_bytes": 42
            },
            {
              "path": "data/v1/modifiers/reforges.json",
              "category": "modifiers",
              "table_name": "reforge",
              "model_class": "dev.sbs.minecraftapi.model.Reforge",
              "content_sha256": "feedface",
              "bytes": 1024,
              "has_extra": false
            }
          ]
        }
        """;

    private final @NotNull Gson gson = MinecraftApi.getGson();

    @Test
    @DisplayName("loadIndex() parses a valid manifest into a ManifestIndex")
    void loadIndexParsesValidManifest() {
        GitHubIndexProvider provider = new GitHubIndexProvider(
            "skyblock-data",
            new StubContract(path -> SAMPLE_MANIFEST_JSON, GitHubIndexProviderTest::emptyCommitList),
            this.gson
        );

        ManifestIndex manifest = provider.loadIndex();

        assertThat(manifest, is(notNullValue()));
        assertThat(manifest.getVersion(), equalTo(1));
        assertThat(manifest.getCount(), equalTo(2));
        assertThat(manifest.getFiles().size(), equalTo(2));
        assertThat(manifest.getFiles().get(0).getPath(), equalTo("data/v1/items/items.json"));
        assertThat(manifest.getFiles().get(0).isHasExtra(), equalTo(true));
        assertThat(manifest.getFiles().get(1).isHasExtra(), equalTo(false));
    }

    @Test
    @DisplayName("loadIndex() forwards the expected manifest path to the contract")
    void loadIndexForwardsManifestPath() {
        AtomicReference<String> capturedPath = new AtomicReference<>();
        GitHubIndexProvider provider = new GitHubIndexProvider(
            "skyblock-data",
            new StubContract(path -> {
                capturedPath.set(path);
                return SAMPLE_MANIFEST_JSON;
            }, GitHubIndexProviderTest::emptyCommitList),
            this.gson
        );

        provider.loadIndex();

        assertThat(capturedPath.get(), equalTo("data/v1/index.json"));
    }

    @Test
    @DisplayName("loadIndex() wraps SkyBlockDataException in JpaException with sourceId")
    void loadIndexWrapsContractException() {
        GitHubIndexProvider provider = new GitHubIndexProvider(
            "skyblock-data",
            new StubContract(path -> { throw fakeException(404, "Not Found"); }, GitHubIndexProviderTest::emptyCommitList),
            this.gson
        );

        JpaException thrown = assertThrows(JpaException.class, provider::loadIndex);

        assertThat(thrown.getMessage(), containsString("skyblock-data"));
        assertThat(thrown.getMessage(), containsString("404"));
        assertThat(thrown.getCause(), is(notNullValue()));
    }

    @Test
    @DisplayName("loadIndex() raises JpaException on empty manifest body")
    void loadIndexRejectsEmptyBody() {
        GitHubIndexProvider provider = new GitHubIndexProvider(
            "skyblock-data",
            new StubContract(path -> "", GitHubIndexProviderTest::emptyCommitList),
            this.gson
        );

        JpaException thrown = assertThrows(JpaException.class, provider::loadIndex);
        assertThat(thrown.getMessage(), containsString("empty or unparseable"));
    }

    // --- stub plumbing below --- //

    private static @NotNull ConcurrentList<GitHubCommit> emptyCommitList() {
        return Concurrent.newUnmodifiableList();
    }

    /**
     * Builds a stub SkyBlockDataException via a lightweight feign.Response.
     *
     * <p>Constructs a real {@code feign.Response} via the Feign builder API and passes it into
     * the production constructor. No Mockito, no network.
     */
    private static @NotNull SkyBlockDataException fakeException(int status, @NotNull String reason) {
        feign.Request request = feign.Request.create(
            feign.Request.HttpMethod.GET,
            "https://api.github.com/repos/skyblock-simplified/skyblock-data/contents/data/v1/index.json",
            java.util.Map.of(),
            feign.Request.Body.empty(),
            new feign.RequestTemplate()
        );
        feign.Response response = feign.Response.builder()
            .status(status)
            .reason(reason)
            .request(request)
            .headers(java.util.Map.of())
            .body("{\"message\":\"" + reason + "\",\"documentation_url\":\"\"}", java.nio.charset.StandardCharsets.UTF_8)
            .build();
        return new SkyBlockDataException("GET /contents/data/v1/index.json", response);
    }

    /**
     * Hand-rolled {@link SkyBlockDataContract} stub that routes
     * {@code getFileContent} and {@code getLatestMasterCommit} calls to the supplied
     * lambdas. Implements the contract interface directly, no proxy.
     */
    private record StubContract(
        @NotNull java.util.function.Function<String, String> fileContent,
        @NotNull java.util.function.Supplier<ConcurrentList<GitHubCommit>> latestCommit
    ) implements SkyBlockDataContract {

        @Override
        public @NotNull ConcurrentList<GitHubCommit> getLatestMasterCommit() {
            return this.latestCommit.get();
        }

        @Override
        public @NotNull String getFileContent(@NotNull String path) {
            return this.fileContent.apply(path);
        }

    }

}
