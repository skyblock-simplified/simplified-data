package dev.sbs.simplifieddata.client.exception;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Unit tests for the 403/429 disambiguation helpers on {@link SkyBlockDataException}.
 *
 * <p>Every test constructs a {@link feign.Response} with hand-crafted status + headers + body
 * and verifies the helper methods return the correct classification. No network I/O.
 */
class SkyBlockDataExceptionTest {

    @Test
    @DisplayName("isPrimaryRateLimit() true when 403 + x-ratelimit-remaining: 0 + message matches")
    void primaryRateLimit403() {
        SkyBlockDataException ex = build(
            403,
            Map.of("x-ratelimit-remaining", List.of("0")),
            "{\"message\":\"API rate limit exceeded for user ID 1234567.\",\"documentation_url\":\"\"}"
        );

        assertThat(ex.isPrimaryRateLimit(), is(true));
        assertThat(ex.isPermissions(), is(false));
        assertThat(ex.isSecondaryRateLimit(), is(false));
    }

    @Test
    @DisplayName("isPrimaryRateLimit() true when 429 + x-ratelimit-remaining: 0 + message matches")
    void primaryRateLimit429() {
        SkyBlockDataException ex = build(
            429,
            Map.of("x-ratelimit-remaining", List.of("0")),
            "{\"message\":\"API rate limit exceeded\",\"documentation_url\":\"\"}"
        );

        assertThat(ex.isPrimaryRateLimit(), is(true));
    }

    @Test
    @DisplayName("isPermissions() true on 403 with non-zero remaining and non-rate-limit message")
    void permissions403() {
        SkyBlockDataException ex = build(
            403,
            Map.of("x-ratelimit-remaining", List.of("4999")),
            "{\"message\":\"Resource not accessible by personal access token\",\"documentation_url\":\"\"}"
        );

        assertThat(ex.isPermissions(), is(true));
        assertThat(ex.isPrimaryRateLimit(), is(false));
        assertThat(ex.isSecondaryRateLimit(), is(false));
    }

    @Test
    @DisplayName("isSecondaryRateLimit() true when message contains 'secondary rate limit'")
    void secondaryRateLimit() {
        SkyBlockDataException ex = build(
            403,
            Map.of("x-ratelimit-remaining", List.of("4999"), "retry-after", List.of("60")),
            "{\"message\":\"You have exceeded a secondary rate limit\",\"documentation_url\":\"\"}"
        );

        assertThat(ex.isSecondaryRateLimit(), is(true));
        assertThat(ex.isPermissions(), is(false));
    }

    @Test
    @DisplayName("getGithubResponse() falls back to Unknown sentinel on non-JSON body")
    void getGithubResponseFallback() {
        SkyBlockDataException ex = build(500, Map.of(), "<html>503 oops</html>");

        assertThat(ex.getGithubResponse().getReason(), equalTo("Unknown (body missing or not JSON)"));
    }

    @Test
    @DisplayName("getGithubResponse().getReason() returns the parsed message")
    void getGithubResponseParsed() {
        SkyBlockDataException ex = build(
            404,
            Map.of(),
            "{\"message\":\"Not Found\",\"documentation_url\":\"https://docs.github.com\"}"
        );

        assertThat(ex.getGithubResponse().getReason(), equalTo("Not Found"));
    }

    private static SkyBlockDataException build(int status, Map<String, List<String>> headers, String body) {
        feign.Request request = feign.Request.create(
            feign.Request.HttpMethod.GET,
            "https://api.github.com/repos/skyblock-simplified/skyblock-data/contents/data/v1/index.json",
            Map.of(),
            feign.Request.Body.empty(),
            new feign.RequestTemplate()
        );
        // feign.Response.Builder.headers expects Map<String, Collection<String>>; List<String> is a Collection<String>.
        Map<String, Collection<String>> headerMap = new HashMap<>();
        headers.forEach(headerMap::put);
        feign.Response response = feign.Response.builder()
            .status(status)
            .reason("test")
            .request(request)
            .headers(headerMap)
            .body(body, StandardCharsets.UTF_8)
            .build();
        return new SkyBlockDataException("GET /test", response);
    }

}
