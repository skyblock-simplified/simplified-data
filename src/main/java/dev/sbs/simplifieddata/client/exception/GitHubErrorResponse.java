package dev.sbs.simplifieddata.client.exception;

import com.google.gson.annotations.SerializedName;
import dev.simplified.client.exception.ApiErrorResponse;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;

/**
 * Parsed error body returned by the GitHub REST API on every non-2xx response.
 *
 * <p>GitHub emits a consistent shape for every failure:
 * <pre>{@code
 * {
 *   "message": "API rate limit exceeded for user ID 1234567.",
 *   "documentation_url": "https://docs.github.com/rest/overview/rate-limits-for-the-rest-api"
 * }
 * }</pre>
 *
 * <p>This mirror implements {@link ApiErrorResponse} so that the framework's
 * {@link dev.simplified.client.exception.ApiException#getResponse()} accessor returns a usable
 * instance. The framework interface requires a single {@code getReason()} accessor; Phase 4b
 * maps {@code reason} to the parsed {@code message} field so GitHub's wording is preserved
 * verbatim.
 *
 * @see ApiErrorResponse
 * @see SkyBlockDataException
 */
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class GitHubErrorResponse implements ApiErrorResponse {

    /** The human-readable error message emitted by GitHub. */
    @SerializedName("message")
    protected @NotNull String message = "Unknown";

    /** The URL to GitHub's documentation for this error class. May be empty. */
    @SerializedName("documentation_url")
    protected @NotNull String documentationUrl = "";

    /**
     * Returns the parsed {@code message} field as the framework-required reason string.
     *
     * @return the error message
     */
    @Override
    public @NotNull String getReason() {
        return this.message;
    }

    /**
     * Returns a sentinel instance for cases where the error body is missing or not valid JSON.
     *
     * @return a placeholder with the message {@code "Unknown (body missing or not JSON)"}
     */
    public static @NotNull GitHubErrorResponse unknown() {
        Unknown sentinel = new Unknown();
        sentinel.message = "Unknown (body missing or not JSON)";
        return sentinel;
    }

    /**
     * Trivial subtype used as the sentinel when no error body is available.
     *
     * <p>Mirrors the {@code HypixelErrorResponse.Unknown} pattern for consistency.
     */
    public static final class Unknown extends GitHubErrorResponse {

        /** Package-private - construct via {@link GitHubErrorResponse#unknown()}. */
        Unknown() {
            super();
        }

    }

}
