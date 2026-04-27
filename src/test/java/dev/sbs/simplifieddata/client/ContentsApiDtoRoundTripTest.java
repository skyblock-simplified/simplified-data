package dev.sbs.simplifieddata.client;

import com.google.gson.Gson;
import dev.sbs.simplifieddata.DataApi;
import dev.sbs.simplifieddata.client.request.PutContentRequest;
import dev.sbs.simplifieddata.client.response.GitHubContentEnvelope;
import dev.sbs.simplifieddata.client.response.GitHubPutResponse;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Gson round-trip tests for the Phase 6b Contents API write-path DTOs. Covers
 * {@link GitHubContentEnvelope}, {@link PutContentRequest}, and
 * {@link GitHubPutResponse}.
 *
 * <p>Every test uses canned JSON fixtures lifted from GitHub's
 * <a href="https://docs.github.com/en/rest/repos/contents?apiVersion=2022-11-28">
 * Contents API documentation</a>, narrowed to the fields Phase 6b consumes.
 * No network I/O; no Feign proxy construction; no Spring context.
 */
class ContentsApiDtoRoundTripTest {

    private static final @NotNull Gson GSON = DataApi.getGson();

    @Test
    @DisplayName("GitHubContentEnvelope deserializes all declared fields from a narrowed envelope fixture")
    void contentEnvelopeFromJson() {
        String json = """
            {
              "name": "zodiac_events.json",
              "path": "data/v1/world/zodiac_events.json",
              "sha": "3d21ec53a331a6f037a91c368710b99387d012c1",
              "size": 1247,
              "content": "W3siaWQiOiJZRUFSX09GX1RIRV9TRUFMIn1d\\n",
              "encoding": "base64"
            }
            """;

        GitHubContentEnvelope envelope = GSON.fromJson(json, GitHubContentEnvelope.class);

        assertThat(envelope, notNullValue());
        assertThat(envelope.getName(), equalTo("zodiac_events.json"));
        assertThat(envelope.getPath(), equalTo("data/v1/world/zodiac_events.json"));
        assertThat(envelope.getSha(), equalTo("3d21ec53a331a6f037a91c368710b99387d012c1"));
        assertThat(envelope.getSize(), equalTo(1247L));
        assertThat(envelope.getContent(), containsString("W3siaWQi"));
        assertThat(envelope.getEncoding(), equalTo("base64"));
    }

    @Test
    @DisplayName("PutContentRequest serializes the four required fields and omits null optionals")
    void putContentRequestToJson() {
        PutContentRequest body = PutContentRequest.builder()
            .message("Update ZodiacEvent: 1 mutation")
            .content("W3siaWQiOiJZRUFSX09GX1RIRV9TRUFMIn1d")
            .sha("3d21ec53a331a6f037a91c368710b99387d012c1")
            .branch("master")
            .build();

        String json = GSON.toJson(body);

        assertThat(json, containsString("\"message\":\"Update ZodiacEvent: 1 mutation\""));
        assertThat(json, containsString("\"content\":\"W3siaWQiOiJZRUFSX09GX1RIRV9TRUFMIn1d\""));
        assertThat(json, containsString("\"sha\":\"3d21ec53a331a6f037a91c368710b99387d012c1\""));
        assertThat(json, containsString("\"branch\":\"master\""));
    }

    @Test
    @DisplayName("PutContentRequest builder handles a null branch and null committer without serializing them")
    void putContentRequestOmitsNulls() {
        PutContentRequest body = PutContentRequest.builder()
            .message("m")
            .content("c")
            .sha("s")
            .build();

        assertThat(body.getBranch(), nullValue());
        assertThat(body.getCommitter(), nullValue());
    }

    @Test
    @DisplayName("PutContentRequest.Committer builder populates name + email")
    void putContentRequestCommitterBuilder() {
        PutContentRequest.Committer committer = PutContentRequest.Committer.builder()
            .name("simplified-data-bot")
            .email("bot@skyblock-simplified.dev")
            .build();

        assertThat(committer.getName(), equalTo("simplified-data-bot"));
        assertThat(committer.getEmail(), equalTo("bot@skyblock-simplified.dev"));
    }

    @Test
    @DisplayName("GitHubPutResponse deserializes nested content.sha and commit.sha for observability logging")
    void putResponseFromJson() {
        String json = """
            {
              "content": {
                "name": "zodiac_events.json",
                "path": "data/v1/world/zodiac_events.json",
                "sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "size": 1247
              },
              "commit": {
                "sha": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "message": "Update ZodiacEvent: 1 mutation"
              }
            }
            """;

        GitHubPutResponse response = GSON.fromJson(json, GitHubPutResponse.class);

        assertThat(response, notNullValue());
        assertThat(response.getContent().getSha(), equalTo("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
        assertThat(response.getCommit().getSha(), equalTo("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));
    }

}
