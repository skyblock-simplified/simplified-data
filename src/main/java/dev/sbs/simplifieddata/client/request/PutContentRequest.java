package dev.sbs.simplifieddata.client.request;

import com.google.gson.annotations.SerializedName;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Request body for the Phase 6b {@code PUT /repos/{owner}/{repo}/contents/{path}}
 * call on {@link dev.sbs.simplifieddata.client.SkyBlockDataWriteContract}.
 *
 * <p>Serialized to JSON by Feign's {@code GsonEncoder} on the outbound request.
 * Every field listed here is a literal GitHub Contents API field - no
 * framework-specific metadata is added. {@link #sha} is the
 * optimistic-concurrency token captured from a prior
 * {@link dev.sbs.simplifieddata.client.response.GitHubContentEnvelope#getSha()
 * content envelope} fetch; omitting it turns the PUT into an unconditional
 * upsert and is explicitly avoided by the Phase 6b path.
 *
 * <p>Instances are built via Lombok's {@code @Builder} to keep call sites tidy
 * at the {@code WritableRemoteJsonSource.commitBatch()} site where the content
 * bytes are freshly base64-encoded for every PUT. The {@link #branch} and
 * {@link #committer} fields are optional per GitHub's API and default to
 * {@code null} here - omitted from the serialized payload by Gson's default
 * behavior.
 *
 * @see <a href="https://docs.github.com/en/rest/repos/contents?apiVersion=2022-11-28#create-or-update-file-contents">
 *      GitHub create or update file contents</a>
 */
@Getter
@Builder
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public final class PutContentRequest {

    /** The commit message body written to the git log. */
    @SerializedName("message")
    private final @NotNull String message;

    /** The new file content, base64-encoded per GitHub's Contents API convention. */
    @SerializedName("content")
    private final @NotNull String content;

    /**
     * The expected blob SHA of the file at the branch tip. GitHub rejects the
     * request with {@code 409}/{@code 412} when this does not match, enabling
     * optimistic-concurrency control for the batched write path.
     */
    @SerializedName("sha")
    private final @NotNull String sha;

    /** Optional target branch name; defaults to the repo default branch when omitted. */
    @SerializedName("branch")
    private final @Nullable String branch;

    /** Optional committer metadata; defaults to the authenticated user when omitted. */
    @SerializedName("committer")
    private final @Nullable Committer committer;

    /**
     * Nested committer identity block on a {@link PutContentRequest}. Carries the
     * name and email displayed in the resulting git commit. Phase 6b omits this by
     * default and lets GitHub fall back to the authenticated PAT user.
     */
    @Getter
    @Builder
    @RequiredArgsConstructor(access = AccessLevel.PACKAGE)
    public static final class Committer {

        /** The display name attached to the commit author field. */
        @SerializedName("name")
        private final @NotNull String name;

        /** The email address attached to the commit author field. */
        @SerializedName("email")
        private final @NotNull String email;

    }

}
