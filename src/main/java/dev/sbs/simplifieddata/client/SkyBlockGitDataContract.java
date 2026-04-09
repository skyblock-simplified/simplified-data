package dev.sbs.simplifieddata.client;

import dev.sbs.simplifieddata.client.exception.SkyBlockDataException;
import dev.sbs.simplifieddata.client.request.CreateBlobRequest;
import dev.sbs.simplifieddata.client.request.CreateCommitRequest;
import dev.sbs.simplifieddata.client.request.CreateTreeRequest;
import dev.sbs.simplifieddata.client.request.UpdateRefRequest;
import dev.sbs.simplifieddata.client.response.GitBlob;
import dev.sbs.simplifieddata.client.response.GitCommit;
import dev.sbs.simplifieddata.client.response.GitRef;
import dev.sbs.simplifieddata.client.response.GitTree;
import dev.simplified.client.request.Contract;
import dev.simplified.client.route.Route;
import feign.Param;
import feign.RequestLine;
import org.jetbrains.annotations.NotNull;

/**
 * Dormant Feign contract exposing the GitHub Git Data API surface for
 * {@code skyblock-simplified/skyblock-data}.
 *
 * <p><b>No production caller in Phase 6b.</b> The Phase 6b write path uses the
 * single-file {@link SkyBlockDataWriteContract} (Contents API) because the
 * complete 7-step Git Data API flow (getRef &rarr; getCommit &rarr; createBlob{N}
 * &rarr; createTree &rarr; createCommit &rarr; updateRef) is significantly more
 * complex and not justified for the initial scope. This contract ships
 * alongside the write path as an extractable API surface for future
 * initiatives: (a) a Phase 6e follow-up that coalesces a
 * {@code WriteBatchScheduler} tick into a single multi-file commit; (b) any
 * downstream project that needs a GitHub Git Data client without duplicating
 * the Feign wiring.
 *
 * <p>The contract is wired via the same
 * {@link dev.sbs.simplifieddata.config.GitHubConfig#skyBlockGitDataClient}
 * bean as the write-path client - same {@code Accept: application/vnd.github+json}
 * static header, same {@code X-GitHub-Api-Version: 2022-11-28} pin, same
 * {@code Authorization} dynamic-header supplier - so a single PAT covers
 * both contracts.
 *
 * <p>Every method is read-only or idempotent with respect to the target branch
 * until a follow-up {@link #updateRef(String, UpdateRefRequest)} call is made.
 * {@code createBlob}, {@code createTree}, and {@code createCommit} all produce
 * detached git objects that are not reachable from any branch until explicitly
 * referenced.
 *
 * <p>DTO round-trip coverage lives in
 * {@code GitDataDtoRoundTripTest}; no live-network tests against the Git Data
 * API run in Phase 6b.
 *
 * @see <a href="https://docs.github.com/en/rest/git?apiVersion=2022-11-28">GitHub Git Database API</a>
 * @see SkyBlockDataWriteContract
 */
@Route("api.github.com")
public interface SkyBlockGitDataContract extends Contract {

    /**
     * Fetches the git reference for a branch by name.
     *
     * <p>Accepts the short branch name (e.g. {@code "master"}), not the
     * fully-qualified ref path (e.g. {@code "refs/heads/master"}) - the path
     * prefix is hardcoded in the {@code @RequestLine} template.
     *
     * @param branch the branch name, e.g. {@code "master"}
     * @return the current ref including the target commit SHA
     * @throws SkyBlockDataException on any non-2xx status
     */
    @RequestLine("GET /repos/skyblock-simplified/skyblock-data/git/refs/heads/{branch}")
    @NotNull GitRef getRef(@Param("branch") @NotNull String branch) throws SkyBlockDataException;

    /**
     * Fetches a git commit object by SHA.
     *
     * <p>Returns the Git Data API commit envelope, which is narrower than the
     * Commits REST API envelope carried by
     * {@link dev.sbs.simplifieddata.client.response.GitHubCommit}. The commit
     * carries a reference to its tree via
     * {@link GitCommit.TreeRef#getSha()}, which is the starting point for the
     * Git Data API write path's {@code createTree} overlay.
     *
     * @param sha the commit SHA to fetch
     * @return the commit object
     * @throws SkyBlockDataException on any non-2xx status
     */
    @RequestLine("GET /repos/skyblock-simplified/skyblock-data/git/commits/{sha}")
    @NotNull GitCommit getCommit(@Param("sha") @NotNull String sha) throws SkyBlockDataException;

    /**
     * Fetches a git tree object by SHA.
     *
     * <p>The {@code recursive} query parameter controls whether nested
     * subtrees are expanded inline. Setting {@code recursive=1} returns every
     * entry in the tree hierarchy up to GitHub's truncation cap; omitting it
     * returns only the top-level entries, with subtrees represented as SHA
     * references that require follow-up calls.
     *
     * @param sha the tree SHA
     * @param recursive pass {@code "1"} to expand subtrees inline, or
     *                  {@code "0"}/empty to fetch only the top-level entries
     * @return the tree object
     * @throws SkyBlockDataException on any non-2xx status
     */
    @RequestLine("GET /repos/skyblock-simplified/skyblock-data/git/trees/{sha}?recursive={recursive}")
    @NotNull GitTree getTree(
        @Param("sha") @NotNull String sha,
        @Param("recursive") @NotNull String recursive
    ) throws SkyBlockDataException;

    /**
     * Creates a new git blob from the supplied content.
     *
     * <p>The returned {@link GitBlob} carries only the SHA and URL - the
     * content, size, and encoding fields are null on create responses. The
     * SHA is the input to a follow-up {@code createTree} call's entries.
     *
     * @param body the blob content and encoding
     * @return the created blob reference
     * @throws SkyBlockDataException on any non-2xx status
     */
    @RequestLine("POST /repos/skyblock-simplified/skyblock-data/git/blobs")
    @NotNull GitBlob createBlob(@NotNull CreateBlobRequest body) throws SkyBlockDataException;

    /**
     * Creates a new git tree from the supplied entries, optionally layered on
     * top of an existing base tree.
     *
     * @param body the tree entries plus optional {@code base_tree} SHA
     * @return the created tree object
     * @throws SkyBlockDataException on any non-2xx status
     */
    @RequestLine("POST /repos/skyblock-simplified/skyblock-data/git/trees")
    @NotNull GitTree createTree(@NotNull CreateTreeRequest body) throws SkyBlockDataException;

    /**
     * Creates a new git commit from the supplied tree and parent SHAs.
     *
     * <p>The commit is detached - no branch points at it until a follow-up
     * {@link #updateRef(String, UpdateRefRequest)} call moves a ref to this
     * commit's SHA.
     *
     * @param body the commit metadata plus tree and parent SHAs
     * @return the created commit object
     * @throws SkyBlockDataException on any non-2xx status
     */
    @RequestLine("POST /repos/skyblock-simplified/skyblock-data/git/commits")
    @NotNull GitCommit createCommit(@NotNull CreateCommitRequest body) throws SkyBlockDataException;

    /**
     * Moves a branch ref to a new commit SHA via {@code PATCH git/refs/heads/{branch}}.
     *
     * <p>When {@link UpdateRefRequest#getForce()} is {@code null} or
     * {@code false}, GitHub enforces a fast-forward check and rejects the
     * update with {@code 422} if the new commit is not a descendant of the
     * current tip. This is the optimistic-concurrency hook for the Phase 6e
     * batched write path.
     *
     * @param branch the branch name
     * @param body the new SHA and optional force flag
     * @return the updated ref reflecting the new target
     * @throws SkyBlockDataException on any non-2xx status
     */
    @RequestLine("PATCH /repos/skyblock-simplified/skyblock-data/git/refs/heads/{branch}")
    @NotNull GitRef updateRef(
        @Param("branch") @NotNull String branch,
        @NotNull UpdateRefRequest body
    ) throws SkyBlockDataException;

}
