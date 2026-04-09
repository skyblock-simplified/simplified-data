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
 * {@code POST /repos/{owner}/{repo}/git/trees} endpoint on
 * {@link dev.sbs.simplifieddata.client.SkyBlockGitDataContract}.
 *
 * <p>A tree is a directory snapshot composed of {@link TreeEntry} entries.
 * The optional {@link #baseTree} reference overlays new entries on top of an
 * existing tree - the Phase 6e write path would set {@code base_tree} to the
 * current master branch's tree SHA and include only the affected files in
 * {@link #tree}, letting the unaffected entries inherit from the base.
 *
 * <p>No production caller in Phase 6b. Shipped with the dormant contract
 * surface.
 *
 * @see <a href="https://docs.github.com/en/rest/git/trees?apiVersion=2022-11-28#create-a-tree">
 *      GitHub create a tree</a>
 */
@Getter
@Builder
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public final class CreateTreeRequest {

    /**
     * Optional base tree SHA. When set, the request overlays {@link #tree} on
     * top of this base; when null, the request creates a standalone tree from
     * only the supplied entries.
     */
    @SerializedName("base_tree")
    private final @Nullable String baseTree;

    /** The new tree entries. */
    @SerializedName("tree")
    private final @NotNull ConcurrentList<TreeEntry> tree;

    /**
     * A single entry inside a {@link CreateTreeRequest}. Exactly one of
     * {@link #sha} (reference an existing blob/tree) or {@link #content}
     * (inline file content) must be non-null per GitHub's API.
     */
    @Getter
    @Builder
    @RequiredArgsConstructor(access = AccessLevel.PACKAGE)
    public static final class TreeEntry {

        /** The entry path relative to the tree root. */
        @SerializedName("path")
        private final @NotNull String path;

        /**
         * The git mode: {@code "100644"} regular file, {@code "100755"} executable,
         * {@code "040000"} tree, {@code "160000"} submodule, {@code "120000"} symlink.
         */
        @SerializedName("mode")
        private final @NotNull String mode;

        /** The entry type: {@code "blob"}, {@code "tree"}, or {@code "commit"}. */
        @SerializedName("type")
        private final @NotNull String type;

        /**
         * The SHA of the existing blob or tree this entry should point at.
         * Exactly one of {@link #sha} or {@link #content} must be set.
         */
        @SerializedName("sha")
        private final @Nullable String sha;

        /**
         * Inline file content as a UTF-8 string. When set, GitHub creates a
         * new blob for the entry implicitly. Exactly one of {@link #sha} or
         * {@link #content} must be set.
         */
        @SerializedName("content")
        private final @Nullable String content;

    }

}
