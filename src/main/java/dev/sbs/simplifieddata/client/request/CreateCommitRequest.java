package dev.sbs.simplifieddata.client.request;

import com.google.gson.annotations.SerializedName;
import dev.simplified.collection.ConcurrentList;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Request body for the dormant Git Data API
 * {@code POST /repos/{owner}/{repo}/git/commits} endpoint on
 * {@link dev.sbs.simplifieddata.client.SkyBlockGitDataContract}.
 *
 * <p>Creates a new commit object pointing at the given tree, with the
 * specified parent commits and commit message. No ref is updated - a separate
 * {@code updateRef} call moves the branch pointer to the new commit.
 *
 * <p>No production caller in Phase 6b. Shipped with the dormant contract
 * surface.
 *
 * @see <a href="https://docs.github.com/en/rest/git/commits?apiVersion=2022-11-28#create-a-commit">
 *      GitHub create a commit</a>
 */
@Getter
@Builder
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public final class CreateCommitRequest {

    /** The commit message. */
    @SerializedName("message")
    private final @NotNull String message;

    /** The SHA of the tree the commit should point at. */
    @SerializedName("tree")
    private final @NotNull String tree;

    /**
     * Parent commit SHAs. An empty list produces a root commit; a single entry
     * is the normal case; more than one produces a merge commit.
     */
    @SerializedName("parents")
    private final @NotNull ConcurrentList<String> parents;

    /** Optional author actor. When null, GitHub populates it from the authenticated PAT user. */
    @SerializedName("author")
    private final @Nullable Actor author;

    /** Optional committer actor. When null, GitHub populates it from the authenticated PAT user. */
    @SerializedName("committer")
    private final @Nullable Actor committer;

    /**
     * Author/committer identity block. Matches the shape of
     * {@link dev.sbs.simplifieddata.client.response.GitCommit.Actor} but lives
     * on the request side.
     */
    @Getter
    @Builder
    @RequiredArgsConstructor(access = AccessLevel.PACKAGE)
    public static final class Actor {

        /** The display name. */
        @SerializedName("name")
        private final @NotNull String name;

        /** The email address. */
        @SerializedName("email")
        private final @NotNull String email;

        /** Optional ISO-8601 timestamp. When null, GitHub uses the current server time. */
        @SerializedName("date")
        private final @Nullable String date;

    }

}
