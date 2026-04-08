package dev.sbs.simplifieddata.source;

import com.google.gson.Gson;
import dev.sbs.simplifieddata.client.SkyBlockDataContract;
import dev.sbs.simplifieddata.client.exception.SkyBlockDataException;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.source.IndexProvider;
import dev.simplified.persistence.source.ManifestIndex;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.jetbrains.annotations.NotNull;

/**
 * Loads the {@code skyblock-data/data/v1/index.json} manifest via the GitHub REST API Contents
 * endpoint.
 *
 * <p>Implements the Phase 4a {@link IndexProvider} SAM so it can feed directly into a
 * {@link dev.simplified.persistence.source.RemoteJsonSource}. Every {@link #loadIndex()} call
 * issues one HTTP GET to {@code /repos/skyblock-simplified/skyblock-data/contents/data/v1/index.json}
 * and parses the response body (the raw manifest JSON) into a {@link ManifestIndex} via the
 * Gson instance supplied at construction time.
 *
 * <p>Phase 4b performs zero caching. Phase 4c will layer change detection on top via the
 * {@code ExternalAssetState} / {@code ExternalAssetEntryState} tables from Phase 4a and a
 * scheduled poller.
 *
 * <p>Any {@link SkyBlockDataException} raised by the contract (non-2xx HTTP status) is
 * re-thrown wrapped in {@link JpaException} per the SAM contract, with the HTTP status code
 * and the source id carried in the message and the original exception retained as the cause.
 *
 * @see IndexProvider
 * @see dev.simplified.persistence.source.RemoteJsonSource
 */
@Log4j2
@RequiredArgsConstructor
public final class GitHubIndexProvider implements IndexProvider {

    /** The path to the manifest file inside the {@code skyblock-data} repo, relative to repo root. */
    private static final @NotNull String MANIFEST_PATH = "data/v1/index.json";

    /** The human-readable source id matching {@code ExternalAssetState.sourceId} in Phase 4c. */
    private final @NotNull String sourceId;

    /** The Feign contract proxy for the GitHub REST API. */
    private final @NotNull SkyBlockDataContract contract;

    /** The Gson instance used to deserialize the manifest body into a {@link ManifestIndex}. */
    private final @NotNull Gson gson;

    @Override
    public @NotNull ManifestIndex loadIndex() throws JpaException {
        try {
            String rawJson = this.contract.getFileContent(MANIFEST_PATH);
            ManifestIndex manifest = this.gson.fromJson(rawJson, ManifestIndex.class);

            if (manifest == null)
                throw new JpaException("GitHub returned empty or unparseable manifest for source '%s'", this.sourceId);

            log.debug("Loaded manifest for source '{}' - {} entries", this.sourceId, manifest.getCount());
            return manifest;
        } catch (SkyBlockDataException ex) {
            throw new JpaException(
                ex,
                "Failed to load manifest for source '%s' (HTTP %d): %s",
                this.sourceId,
                ex.getStatus().getCode(),
                ex.getGithubResponse().getReason()
            );
        }
    }

}
