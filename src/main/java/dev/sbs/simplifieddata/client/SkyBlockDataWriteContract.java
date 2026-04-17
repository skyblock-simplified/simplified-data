package dev.sbs.simplifieddata.client;

import dev.sbs.simplifieddata.client.exception.SkyBlockDataException;
import dev.sbs.simplifieddata.client.request.PutContentRequest;
import dev.sbs.simplifieddata.client.response.GitHubContentEnvelope;
import dev.sbs.simplifieddata.client.response.GitHubPutResponse;
import dev.sbs.simplifieddata.config.GitHubConfig;
import dev.simplified.client.ClientConfig;
import dev.simplified.client.request.Contract;
import dev.simplified.client.route.Route;
import feign.Param;
import feign.RequestLine;
import org.jetbrains.annotations.NotNull;

/**
 * Phase 6b Feign contract for the GitHub Contents API write path against the
 * {@code skyblock-data} repository.
 *
 * <p>This interface is deliberately a sibling of the existing
 * {@link SkyBlockDataContract} rather than an extension of it. The split is
 * load-bearing: the existing read-path contract is wired to a client with
 * {@code Accept: application/vnd.github.raw+json} as its static header so the
 * Contents endpoint returns the raw file body directly. This interface's
 * companion client in {@link GitHubConfig#skyBlockDataWriteClient} uses
 * {@code Accept: application/vnd.github+json} so the Contents endpoint returns
 * the JSON envelope (including the blob {@code sha} field needed as an
 * optimistic-concurrency token) and the PUT endpoint accepts a standard JSON
 * body. Merging both contracts onto a single client would attach two
 * {@code Accept} headers to every request (see
 * {@code Client.buildInternalClient()} at {@code Client.java:322} which calls
 * {@code request.addHeader(...)} - Apache HttpClient treats duplicate
 * {@code addHeader} calls as additive rather than replacing), which produces
 * brittle content-negotiation behavior.
 *
 * <p>Authentication, the {@code X-GitHub-Api-Version: 2022-11-28} header, and the
 * {@code If-None-Match} auto-attach path are all shared with the read-path
 * client via the same {@code skyBlockDataAuthorizationSupplier} bean. The only
 * difference between the two clients' {@link ClientConfig}
 * is the {@code Accept} header.
 *
 * <p>No disk overlay interaction happens through this contract. The Phase 6b
 * {@code WritableRemoteJsonSource} reads the current file body via the existing
 * {@link SkyBlockDataContract#getFileContent} path (raw media type, bypasses
 * base64), then uses {@link #getFileMetadata(String)} on this contract purely to
 * read the envelope's {@code sha} field, then uses {@link #putFileContent} on
 * this contract to write the mutated body back.
 *
 * @see SkyBlockDataContract
 * @see GitHubConfig
 */
@Route("api.github.com")
public interface SkyBlockDataWriteContract extends Contract {

    /**
     * Fetches the Contents API JSON envelope for the given repo-root-relative path
     * on the {@code master} branch of {@code skyblock-simplified/skyblock-data}.
     *
     * <p>Unlike {@link SkyBlockDataContract#getFileContent(String)}, this method
     * is wired under the {@code application/vnd.github+json} media type (set as
     * the static {@code Accept} header on {@link GitHubConfig#skyBlockDataWriteClient}),
     * so the response body is the JSON envelope rather than the raw file bytes.
     * The envelope's {@link GitHubContentEnvelope#getSha()} field is the git
     * <b>blob</b> SHA at the branch tip, which the batch write path uses as the
     * optimistic-concurrency token on the follow-up
     * {@link #putFileContent(String, PutContentRequest)} call.
     *
     * <p>The {@code Accept} header difference is the only reason this method
     * lives on a separate contract from
     * {@link SkyBlockDataContract#getFileContent(String)}. See the class-level
     * Javadoc for the rationale.
     *
     * @param path the repo-root-relative file path, e.g. {@code "data/v1/world/zodiac_events.json"}
     * @return the Contents API envelope with {@code sha}, {@code size}, and base64 {@code content}
     * @throws SkyBlockDataException on any non-2xx status
     */
    @RequestLine("GET /repos/skyblock-simplified/skyblock-data/contents/{path}?ref=master")
    @NotNull GitHubContentEnvelope getFileMetadata(@Param("path") @NotNull String path) throws SkyBlockDataException;

    /**
     * Writes a new version of the file at the given path via the Contents API
     * {@code PUT} endpoint, using the supplied blob SHA as the optimistic-concurrency
     * token.
     *
     * <p>The request body must be a fully populated {@link PutContentRequest}
     * with {@link PutContentRequest#getSha()} set to the blob SHA previously
     * observed via {@link #getFileMetadata(String)}. A stale SHA produces a
     * {@code 409 Conflict} response which the framework maps to
     * {@link dev.simplified.client.exception.PreconditionFailedException}. The
     * Phase 6b retry path (see
     * {@code WritableRemoteJsonSource.commitBatch}) catches that exception,
     * refetches the current blob SHA, and retries the PUT up to 3 times before
     * escalating to the exponential-backoff retry path.
     *
     * <p>GitHub produces a new commit on the target branch for every successful
     * PUT, so a {@code commitBatch} tick that touches N distinct files produces
     * N commits (one per file). A future Phase 6e may switch the write path to
     * the {@link SkyBlockGitDataContract} Git Data API so a single commit can
     * span multiple files.
     *
     * @param path the repo-root-relative file path, e.g. {@code "data/v1/world/zodiac_events.json"}
     * @param body the PUT body carrying message, base64 content, and blob SHA
     * @return the GitHub PUT response envelope with the new blob SHA and commit SHA
     * @throws SkyBlockDataException on any non-2xx status (including 409 Conflict for SHA mismatch)
     */
    @RequestLine("PUT /repos/skyblock-simplified/skyblock-data/contents/{path}")
    @NotNull GitHubPutResponse putFileContent(
        @Param("path") @NotNull String path,
        @NotNull PutContentRequest body
    ) throws SkyBlockDataException;

}
