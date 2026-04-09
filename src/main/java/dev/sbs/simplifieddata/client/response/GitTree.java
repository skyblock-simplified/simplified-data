package dev.sbs.simplifieddata.client.response;

import com.google.gson.annotations.SerializedName;
import dev.simplified.collection.ConcurrentList;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Gson-bindable mirror of a git tree object returned by the GitHub Git Data API.
 *
 * <p>Returned by {@code GET /repos/{owner}/{repo}/git/trees/{sha}} and
 * {@code POST /repos/{owner}/{repo}/git/trees}. A tree is a directory-level
 * snapshot of the repository; each {@link Entry} describes a single path
 * entry (blob or subtree) with its mode, type, and SHA.
 *
 * <p>Phase 6b ships this DTO as part of the dormant
 * {@link dev.sbs.simplifieddata.client.SkyBlockGitDataContract} surface - no
 * production code reads or writes it yet. The Phase 6e Git Data API write
 * path would read an existing tree, overlay new blob SHAs onto affected
 * entries, and {@code POST} a new tree with those entries plus the original
 * {@code base_tree} reference.
 *
 * @see <a href="https://docs.github.com/en/rest/git/trees?apiVersion=2022-11-28">GitHub Git trees</a>
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class GitTree {

    /** The tree SHA. */
    @SerializedName("sha")
    private final @NotNull String sha;

    /** The GitHub API URL for the tree. */
    @SerializedName("url")
    private final @NotNull String url;

    /** The tree entries. An empty list is permitted on freshly created trees. */
    @SerializedName("tree")
    private final @NotNull ConcurrentList<Entry> tree;

    /**
     * {@code true} when the tree exceeds GitHub's recursive listing cap and was
     * returned incomplete. Nullable because the field is only present on
     * recursive listings.
     */
    @SerializedName("truncated")
    private final @Nullable Boolean truncated;

    /**
     * A single entry in a git tree - either a file blob or a subtree.
     *
     * <p>Path is the entry name relative to its parent tree; subtrees have
     * nested children reachable by fetching the entry's SHA as a tree. Phase 6e
     * would build these via {@link dev.sbs.simplifieddata.client.request.CreateTreeRequest.TreeEntry}.
     */
    @Getter
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class Entry {

        /** The entry path relative to the parent tree. */
        @SerializedName("path")
        private final @NotNull String path;

        /**
         * The git mode string, e.g. {@code "100644"} for a regular file,
         * {@code "100755"} for an executable, {@code "040000"} for a directory.
         */
        @SerializedName("mode")
        private final @NotNull String mode;

        /** The entry type, typically {@code "blob"} or {@code "tree"}. */
        @SerializedName("type")
        private final @NotNull String type;

        /** The target object SHA. */
        @SerializedName("sha")
        private final @NotNull String sha;

        /** The file size in bytes for blob entries. Null for tree entries. */
        @SerializedName("size")
        private final @Nullable Long size;

        /** The GitHub API URL pointing at the target object. */
        @SerializedName("url")
        private final @NotNull String url;

    }

}
