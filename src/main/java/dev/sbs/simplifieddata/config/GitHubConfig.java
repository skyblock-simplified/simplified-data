package dev.sbs.simplifieddata.config;

import com.google.gson.Gson;
import dev.sbs.minecraftapi.MinecraftApi;
import dev.sbs.simplifieddata.client.SkyBlockDataContract;
import dev.sbs.simplifieddata.client.exception.SkyBlockDataException;
import dev.sbs.simplifieddata.source.GitHubFileFetcher;
import dev.sbs.simplifieddata.source.GitHubIndexProvider;
import dev.simplified.client.Client;
import dev.simplified.client.ClientOptions;
import dev.simplified.persistence.source.FileFetcher;
import dev.simplified.persistence.source.IndexProvider;
import lombok.extern.log4j.Log4j2;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * Wires the GitHub-backed {@code skyblock-data} client and Phase 4a source bridge beans into
 * the {@code simplified-data} Spring context.
 *
 * <p>The PAT is read from the {@code SKYBLOCK_DATA_GITHUB_TOKEN} environment variable via the
 * Spring property placeholder {@code skyblock.data.github.token}. A missing or blank value
 * degrades the client to unauthenticated mode (60 req/hr per IP) and logs a warning rather
 * than failing the context refresh - public repo reads still succeed, which lets dev
 * environments boot without provisioning a token. Production deployments should always set
 * the variable for the 5000 req/hr authenticated budget.
 *
 * <p>Bean graph:
 * <ul>
 *   <li>{@code skyBlockDataAuthorizationSupplier} - Spring-injected supplier of the
 *       {@code Authorization} header value, resolved once per outbound request via the
 *       {@link dev.simplified.client.Client} dynamic-header pipeline.</li>
 *   <li>{@code skyBlockDataClient} - the {@link Client} wrapping the
 *       {@link SkyBlockDataContract} proxy.</li>
 *   <li>{@code gitHubIndexProvider} - the Phase 4a {@link IndexProvider} bridge.</li>
 *   <li>{@code gitHubFileFetcher} - the Phase 4a {@link FileFetcher} bridge.</li>
 * </ul>
 *
 * <p>Phase 5.5.1 note - {@code If-None-Match} conditional requests are handled automatically
 * by the {@code dev.simplified.client} library. {@code InternalRequestInterceptor} attaches
 * the header on every {@code GET} / {@code HEAD} when a matching cached response exists in
 * {@link Client#getRecentResponses()}, and {@code InternalResponseInterceptor} transparently
 * serves the cached body on {@code 304} by synthesizing a fresh response envelope with
 * {@link dev.simplified.client.response.HttpStatus#NOT_MODIFIED} as the wire-truth status.
 * Callers only see a {@link dev.simplified.client.exception.NotModifiedException} on the
 * rare cache-miss revalidation path (client restart, TTL prune, streaming endpoint). No
 * explicit {@code If-None-Match} supplier is needed here - the framework does it.
 *
 * <p>The default {@link dev.simplified.client.request.Timings#cacheDuration()} of one hour
 * and {@code maxCacheSize} of 100 are appropriate for {@code simplified-data}: the 41
 * entity files + the commits API response comfortably fit under the 100 cap, and steady-state
 * no-change polls synthesize additional {@code 304} entries whose borrowed body references
 * keep the original 200 bodies live via GC even after the 200 entries themselves age out.
 * {@code REDIRECTION} is a non-error state, so synthesized 304 responses pass the
 * framework's {@code !response.isError()} cache filter and keep the auto-attach chain alive
 * indefinitely.
 *
 * <p>None of these beans issue any network I/O at construction. The {@link Client} wrapper
 * builds the Feign proxy and Apache HttpClient connection pool lazily on the first contract
 * method invocation, so an unreachable {@code api.github.com} at boot does NOT prevent
 * simplified-data from starting.
 */
@Configuration
@Log4j2
public class GitHubConfig {

    /** The human-readable source id used in exception messages and Phase 4c's asset state. */
    public static final @NotNull String SOURCE_ID = "skyblock-data";

    /** The GitHub REST API version header value locked by the research pack. */
    private static final @NotNull String GITHUB_API_VERSION = "2022-11-28";

    /** The {@code Accept} media type required by the Contents endpoint for files above 1 MB. */
    private static final @NotNull String GITHUB_RAW_ACCEPT = "application/vnd.github.raw+json";

    /**
     * Builds the dynamic supplier of the {@code Authorization} header value from the
     * Spring-resolved PAT property.
     *
     * <p>A blank or missing token returns {@link Optional#empty()} on every invocation so the
     * framework's request interceptor omits the header entirely, leaving the client in
     * unauthenticated mode. A non-blank token returns {@code Bearer <token>} verbatim.
     *
     * @param token the PAT resolved from {@code skyblock.data.github.token} / the
     *              {@code SKYBLOCK_DATA_GITHUB_TOKEN} env var; may be empty
     * @return the dynamic-header supplier
     */
    @Bean
    public @NotNull Supplier<Optional<String>> skyBlockDataAuthorizationSupplier(
        @Value("${skyblock.data.github.token:}") @NotNull String token
    ) {
        if (token.isBlank()) {
            log.warn(
                "SKYBLOCK_DATA_GITHUB_TOKEN is not set - falling back to unauthenticated GitHub "
                    + "access (60 req/hr per IP). Set the environment variable for the 5000 req/hr "
                    + "authenticated budget."
            );
            return Optional::empty;
        }

        String headerValue = "Bearer " + token;
        log.info(
            "SKYBLOCK_DATA_GITHUB_TOKEN loaded (length={}) - GitHub client will use "
                + "authenticated requests (5000 req/hr budget)",
            token.length()
        );
        return () -> Optional.of(headerValue);
    }

    /**
     * Builds the {@link Client} for the {@link SkyBlockDataContract}.
     *
     * <p>Wires three headers:
     * <ul>
     *   <li>{@code Accept} (static) - pinned to {@code application/vnd.github.raw+json} so the
     *       Contents endpoint returns raw file bodies for files above 1 MB. The Commits
     *       endpoint accepts this media type as a no-op alias for its standard JSON.</li>
     *   <li>{@code X-GitHub-Api-Version} (static) - pinned to {@code 2022-11-28}.</li>
     *   <li>{@code Authorization} (dynamic) - sourced from
     *       {@code skyBlockDataAuthorizationSupplier}.</li>
     * </ul>
     *
     * <p>{@code If-None-Match} is handled automatically by the
     * {@code dev.simplified.client.interceptor.InternalRequestInterceptor} auto-attach path
     * introduced in the Phase 5.5.1 client-library update - see the class-level Javadoc.
     * No explicit header wiring is required here.
     *
     * <p>The error decoder maps every non-2xx response to a {@link SkyBlockDataException},
     * matching the {@code HypixelApiException} / {@code SbsApiException} /
     * {@code MojangApiException} pattern in {@code minecraft-api}.
     *
     * <p>The Gson instance is obtained from {@link MinecraftApi#getGson()} so the client
     * reuses the {@code ConcurrentList} type adapter, the {@code JpaExclusionStrategy}, and
     * the SkyBlock type adapters. Creating a fresh {@code new Gson()} here would break
     * {@code ConcurrentList<GitHubCommit>} deserialization on the commit response.
     *
     * @param skyBlockDataAuthorizationSupplier the dynamic {@code Authorization} supplier
     * @return the fully constructed client wrapper; no network I/O performed during build
     */
    @Bean
    public @NotNull Client<SkyBlockDataContract> skyBlockDataClient(
        @Qualifier("skyBlockDataAuthorizationSupplier") @NotNull Supplier<Optional<String>> skyBlockDataAuthorizationSupplier
    ) {
        Gson gson = MinecraftApi.getGson();

        ClientOptions<SkyBlockDataContract> options = ClientOptions.builder(SkyBlockDataContract.class, gson)
            .withHeader("Accept", GITHUB_RAW_ACCEPT)
            .withHeader("X-GitHub-Api-Version", GITHUB_API_VERSION)
            .withDynamicHeader("Authorization", skyBlockDataAuthorizationSupplier)
            .withErrorDecoder((methodKey, response) -> {
                throw new SkyBlockDataException(methodKey, response);
            })
            .build();

        log.info("Building SkyBlockDataContract client against api.github.com");
        return Client.create(options);
    }

    /**
     * Registers the {@link GitHubIndexProvider} bridge bean.
     *
     * @param skyBlockDataClient the GitHub client wrapper
     * @return a Phase 4a {@link IndexProvider} backed by the GitHub Contents API
     */
    @Bean
    public @NotNull IndexProvider gitHubIndexProvider(@NotNull Client<SkyBlockDataContract> skyBlockDataClient) {
        return new GitHubIndexProvider(SOURCE_ID, skyBlockDataClient.getContract(), MinecraftApi.getGson());
    }

    /**
     * Registers the {@link GitHubFileFetcher} bridge bean.
     *
     * @param skyBlockDataClient the GitHub client wrapper
     * @return a Phase 4a {@link FileFetcher} backed by the GitHub Contents API
     */
    @Bean
    public @NotNull FileFetcher gitHubFileFetcher(@NotNull Client<SkyBlockDataContract> skyBlockDataClient) {
        return new GitHubFileFetcher(SOURCE_ID, skyBlockDataClient.getContract());
    }

}
