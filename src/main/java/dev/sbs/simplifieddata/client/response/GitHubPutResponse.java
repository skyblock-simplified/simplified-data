package dev.sbs.simplifieddata.client.response;

import com.google.gson.annotations.SerializedName;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;

/**
 * Gson-bindable mirror of the GitHub response envelope for
 * {@code PUT /repos/{owner}/{repo}/contents/{path}}.
 *
 * <p>Only the two fields Phase 6b logs for observability are declared:
 * the new blob SHA of the updated file (accessible via
 * {@link ContentRef#getSha()}) and the new commit SHA written to git history
 * (accessible via {@link CommitRef#getSha()}). Every other field in the upstream
 * JSON is silently ignored by Gson's reflective binder.
 *
 * <p>The consumer surfaces both SHAs in a single INFO line per successful commit
 * so operators can trace batch writes through the AssetPoller's next targeted
 * refresh cycle.
 *
 * @see <a href="https://docs.github.com/en/rest/repos/contents?apiVersion=2022-11-28#create-or-update-file-contents">
 *      GitHub create or update file contents</a>
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class GitHubPutResponse {

    /** The nested {@code content} object carrying the new file blob SHA. */
    @SerializedName("content")
    private final @NotNull ContentRef content;

    /** The nested {@code commit} object carrying the new git commit SHA. */
    @SerializedName("commit")
    private final @NotNull CommitRef commit;

    /**
     * Narrowed {@code content} sub-envelope. Only {@link #sha} is consumed.
     */
    @Getter
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class ContentRef {

        /** The new blob SHA of the just-written file. Load-bearing for the next PUT's optimistic-concurrency token. */
        @SerializedName("sha")
        private final @NotNull String sha;

    }

    /**
     * Narrowed {@code commit} sub-envelope. Only {@link #sha} is consumed.
     */
    @Getter
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class CommitRef {

        /** The new git commit SHA produced by the PUT. Surfaces in the INFO log so operators can trace the write. */
        @SerializedName("sha")
        private final @NotNull String sha;

    }

}
