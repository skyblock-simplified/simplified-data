package dev.sbs.simplifieddata.client.response;

import com.google.gson.annotations.SerializedName;
import dev.simplified.collection.ConcurrentList;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Gson-bindable mirror of a git commit object returned by the GitHub Git Data API.
 *
 * <p>Returned by {@code GET /repos/{owner}/{repo}/git/commits/{sha}} and
 * {@code POST /repos/{owner}/{repo}/git/commits}. Structurally distinct from
 * {@link GitHubCommit} (the Commits REST API envelope) - this type is narrower
 * because the Git Data API surfaces raw git objects without the repo and
 * committer-HTML additions the Commits API adds.
 *
 * <p>Phase 6b ships this DTO as part of the dormant
 * {@link dev.sbs.simplifieddata.client.SkyBlockGitDataContract} surface - no
 * production code reads or writes it yet. Consumers fetch an existing commit
 * to obtain its tree SHA via {@link TreeRef#getSha()}, then use that as the
 * base tree for a follow-up {@code createTree} call.
 *
 * @see <a href="https://docs.github.com/en/rest/git/commits?apiVersion=2022-11-28">GitHub Git commits</a>
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class GitCommit {

    /** The commit SHA. */
    @SerializedName("sha")
    private final @NotNull String sha;

    /** The commit message body. */
    @SerializedName("message")
    private final @NotNull String message;

    /** Reference to the tree the commit points at. */
    @SerializedName("tree")
    private final @NotNull TreeRef tree;

    /**
     * Parent commit references. An empty list indicates a root commit; a single
     * entry is the normal case; more than one indicates a merge commit.
     */
    @SerializedName("parents")
    private final @NotNull ConcurrentList<ParentRef> parents;

    /** Optional author actor. Nullable for commits synthesized by the Git Data API without an explicit author. */
    @SerializedName("author")
    private final @Nullable Actor author;

    /** Optional committer actor. Nullable for commits synthesized by the Git Data API without an explicit committer. */
    @SerializedName("committer")
    private final @Nullable Actor committer;

    /**
     * Narrowed tree reference embedded in a {@link GitCommit}. Only the SHA is
     * consumed by the dormant surface.
     */
    @Getter
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class TreeRef {

        /** The tree SHA the commit points at. */
        @SerializedName("sha")
        private final @NotNull String sha;

        /** The GitHub API URL for the referenced tree. */
        @SerializedName("url")
        private final @NotNull String url;

    }

    /**
     * Narrowed parent commit reference embedded in a {@link GitCommit}.
     */
    @Getter
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class ParentRef {

        /** The parent commit SHA. */
        @SerializedName("sha")
        private final @NotNull String sha;

        /** The GitHub API URL for the parent commit. */
        @SerializedName("url")
        private final @NotNull String url;

    }

    /**
     * Git Data API actor block - name, email, and ISO-8601 date. Distinct from
     * {@link GitHubCommit.Actor} because the Git Data API omits the API-specific
     * HTML URL fields the Commits REST API adds.
     */
    @Getter
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class Actor {

        /** The display name of the actor. */
        @SerializedName("name")
        private final @NotNull String name;

        /** The email of the actor. */
        @SerializedName("email")
        private final @NotNull String email;

        /** The ISO-8601 UTC timestamp. */
        @SerializedName("date")
        private final @NotNull String date;

    }

}
