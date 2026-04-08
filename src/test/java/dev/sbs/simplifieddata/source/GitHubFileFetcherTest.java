package dev.sbs.simplifieddata.source;

import dev.sbs.simplifieddata.client.SkyBlockDataContract;
import dev.sbs.simplifieddata.client.exception.SkyBlockDataException;
import dev.sbs.simplifieddata.client.response.GitHubCommit;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.exception.JpaException;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link GitHubFileFetcher}. Drives the impl against a hand-rolled
 * {@link SkyBlockDataContract} stub - no network I/O.
 */
class GitHubFileFetcherTest {

    @Test
    @DisplayName("fetchFile() forwards the path verbatim to the contract and returns the body")
    void fetchFileForwardsPath() {
        AtomicReference<String> capturedPath = new AtomicReference<>();
        StubContract stub = new StubContract(path -> {
            capturedPath.set(path);
            return "[{\"id\":1}]";
        });
        GitHubFileFetcher fetcher = new GitHubFileFetcher("skyblock-data", stub);

        String body = fetcher.fetchFile("data/v1/items/items.json");

        assertThat(capturedPath.get(), equalTo("data/v1/items/items.json"));
        assertThat(body, equalTo("[{\"id\":1}]"));
    }

    @Test
    @DisplayName("fetchFile() wraps SkyBlockDataException in JpaException with path + sourceId")
    void fetchFileWrapsContractException() {
        StubContract stub = new StubContract(path -> { throw fakeException(404, "Not Found"); });
        GitHubFileFetcher fetcher = new GitHubFileFetcher("skyblock-data", stub);

        JpaException thrown = assertThrows(JpaException.class, () ->
            fetcher.fetchFile("data/v1/items/items.json")
        );

        assertThat(thrown.getMessage(), containsString("data/v1/items/items.json"));
        assertThat(thrown.getMessage(), containsString("skyblock-data"));
        assertThat(thrown.getMessage(), containsString("404"));
    }

    // --- stub plumbing below --- //

    private static @NotNull SkyBlockDataException fakeException(int status, @NotNull String reason) {
        feign.Request request = feign.Request.create(
            feign.Request.HttpMethod.GET,
            "https://api.github.com/fake",
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
        return new SkyBlockDataException("GET /fake", response);
    }

    private record StubContract(
        @NotNull java.util.function.Function<String, String> fileContent
    ) implements SkyBlockDataContract {

        @Override
        public @NotNull ConcurrentList<GitHubCommit> getLatestMasterCommit() {
            return Concurrent.newUnmodifiableList();
        }

        @Override
        public byte @NotNull [] getFileContent(@NotNull String path) {
            return this.fileContent.apply(path).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }

    }

}
