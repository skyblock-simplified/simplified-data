package dev.sbs.simplifieddata.client.response;

import com.google.gson.annotations.SerializedName;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;

/**
 * Gson-bindable mirror of a git reference returned by the GitHub Git Data API.
 *
 * <p>Shared by {@code GET /repos/{owner}/{repo}/git/refs/heads/{branch}} and
 * {@code PATCH /repos/{owner}/{repo}/git/refs/heads/{branch}} since the response
 * envelopes are structurally identical. The caller reads {@link Object#getSha()}
 * on {@link #object} to get the current commit SHA the branch points at, then
 * uses that as the parent for a follow-up commit in the Git Data API multi-file
 * write path.
 *
 * <p>Phase 6b ships this DTO as part of the dormant
 * {@link dev.sbs.simplifieddata.client.SkyBlockGitDataContract} surface - no
 * production code reads or writes it yet. The Phase 6b write path uses the
 * single-file Contents API. A future Phase 6e may switch to the Git Data API
 * so a single commit can span multiple files in one batch tick.
 *
 * @see <a href="https://docs.github.com/en/rest/git/refs?apiVersion=2022-11-28">GitHub Git refs</a>
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class GitRef {

    /** The fully qualified ref name, e.g. {@code "refs/heads/master"}. */
    @SerializedName("ref")
    private final @NotNull String ref;

    /** The GitHub API URL for this ref. */
    @SerializedName("url")
    private final @NotNull String url;

    /** The target object the ref points at. */
    @SerializedName("object")
    private final @NotNull Object object;

    /**
     * The git object a {@link GitRef} points at - typically a commit, but the
     * envelope also supports tag refs pointing at tag objects (unused by this
     * consumer).
     */
    @Getter
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class Object {

        /** The object SHA - for a branch ref this is the current commit SHA at the tip. */
        @SerializedName("sha")
        private final @NotNull String sha;

        /** The object type, typically {@code "commit"} for branch refs. */
        @SerializedName("type")
        private final @NotNull String type;

        /** The GitHub API URL pointing at the target object. */
        @SerializedName("url")
        private final @NotNull String url;

    }

}
