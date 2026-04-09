package dev.sbs.simplifieddata.client.request;

import com.google.gson.annotations.SerializedName;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Request body for the dormant Git Data API
 * {@code PATCH /repos/{owner}/{repo}/git/refs/heads/{branch}} endpoint on
 * {@link dev.sbs.simplifieddata.client.SkyBlockGitDataContract}.
 *
 * <p>Moves the target branch pointer to the supplied {@link #sha}, optionally
 * with {@link #force} set to {@code true} for non-fast-forward updates. The
 * Phase 6e multi-file write path would set {@code force} to {@code false} and
 * rely on GitHub's native fast-forward check to enforce optimistic concurrency.
 *
 * <p>No production caller in Phase 6b. Shipped with the dormant contract
 * surface.
 *
 * @see <a href="https://docs.github.com/en/rest/git/refs?apiVersion=2022-11-28#update-a-reference">
 *      GitHub update a reference</a>
 */
@Getter
@Builder
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public final class UpdateRefRequest {

    /** The new SHA the branch pointer should be moved to. */
    @SerializedName("sha")
    private final @NotNull String sha;

    /**
     * When {@code true}, a non-fast-forward update is permitted. When
     * {@code false} (the Phase 6e default), GitHub rejects non-fast-forward
     * updates with a {@code 422}, giving the batched write path optimistic
     * concurrency via the ref pointer.
     */
    @SerializedName("force")
    private final @Nullable Boolean force;

}
