package dev.sbs.simplifieddata.persistence;

import com.google.gson.Gson;
import dev.sbs.minecraftapi.persistence.SkyBlockFactory;
import dev.sbs.simplifieddata.client.SkyBlockDataWriteContract;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.collection.ConcurrentMap;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.RepositoryFactory;
import dev.simplified.persistence.source.DiskOverlaySource;
import dev.simplified.persistence.source.FileFetcher;
import dev.simplified.persistence.source.IndexProvider;
import dev.simplified.persistence.source.RemoteJsonSource;
import dev.simplified.persistence.source.Source;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.file.Path;
import java.util.function.Consumer;

/**
 * A {@link RepositoryFactory} that serves SkyBlock models from GitHub via
 * {@link RemoteJsonSource}, with an optional local-disk overlay layer provided by
 * {@link DiskOverlaySource} for dev-mode workflows and a Phase 6b
 * {@link WritableRemoteJsonSource} wrapper on top for write-path buffering.
 *
 * <p>Delegates model enumeration to the existing {@link SkyBlockFactory} bean in
 * {@code minecraft-api} so the authoritative 41-entity list lives in one place. The
 * constructor eagerly instantiates one source chain per model:
 * {@code WritableRemoteJsonSource(DiskOverlaySource(RemoteJsonSource(...)))}. Reads
 * delegate through the disk overlay layer unchanged; writes bypass the delegate
 * and go direct to GitHub via the injected {@link SkyBlockDataWriteContract}.
 *
 * <p>The per-model {@link WritableRemoteJsonSource} instances are stored in both
 * {@link #getSources()} (for Hibernate's repository initialization path) and
 * {@link #getWritableSources()} (for the Phase 6b {@code WriteQueueConsumer}
 * and {@code WriteBatchScheduler} registry lookup). The two maps reference the
 * same underlying instances - the second accessor exists purely to give the
 * write-path beans a typed view of the registry without casting.
 *
 * <p>The overlay path is resolved per model by joining the configured base directory with
 * {@code <@Table.name>.json}. A model class missing a {@link Table} annotation triggers an
 * {@link IllegalStateException} during construction - the failure is deliberate and loud,
 * matching the {@code JsonSource} semantics in the persistence library.
 *
 * <p>This class does zero network I/O at construction. Every bean is a lazy lambda that is
 * only invoked on {@code JpaRepository.refresh()} calls, which happen inside
 * {@link dev.simplified.persistence.JpaSession#cacheRepositories()} and subsequent scheduled
 * refreshes, or on {@code WritableRemoteJsonSource.commitBatch()} calls from the
 * {@code WriteBatchScheduler}. The Phase 4b {@code GitHubIndexProvider} and
 * {@code GitHubFileFetcher} beans are already constructed at this point but have not yet
 * fired any request.
 *
 * @see SkyBlockFactory
 * @see RemoteJsonSource
 * @see DiskOverlaySource
 * @see WritableRemoteJsonSource
 */
@Log4j2
@Getter
public class RemoteSkyBlockFactory implements RepositoryFactory {

    /** Suffix appended to {@code @Table.name} when resolving the per-model overlay file path. */
    private static final @NotNull String OVERLAY_SUFFIX = ".json";

    /** The human-readable source id shared with {@code GitHubConfig.SOURCE_ID} and the asset-state rows. */
    private final @NotNull String sourceId;

    /** Model list delegated to the {@link SkyBlockFactory} bean (already topologically sorted). */
    private final @NotNull ConcurrentList<Class<JpaModel>> models;

    /** Per-model source registrations, populated once at construction time. */
    private final @NotNull ConcurrentMap<Class<?>, Source<?>> sources;

    /**
     * Per-model writable source registrations - the same instances stored in
     * {@link #sources} but narrowed to {@link WritableRemoteJsonSource} so the
     * Phase 6b write-path beans can iterate them without casting.
     */
    private final @NotNull ConcurrentMap<Class<? extends JpaModel>, WritableRemoteJsonSource<?>> writableSources;

    /**
     * Constructs the factory by eagerly wiring one
     * {@link WritableRemoteJsonSource} wrapping one {@link DiskOverlaySource}
     * wrapping one {@link RemoteJsonSource} per model class returned by
     * {@code delegate.getModels()}.
     *
     * @param sourceId the human-readable source id
     * @param delegate the {@link SkyBlockFactory} bean that owns the model list
     * @param indexProvider the Phase 4b {@link IndexProvider} bean
     * @param fileFetcher the Phase 4b {@link FileFetcher} bean
     * @param writeContract the Phase 6b GitHub write-path contract
     * @param gson the Gson instance used to serialize the mutated entity list on PUT
     * @param overlayBasePath the base directory under which per-model overlay files are resolved
     * @param max412ImmediateRetries maximum immediate 412 retries per commit tick
     */
    public RemoteSkyBlockFactory(
        @NotNull String sourceId,
        @NotNull SkyBlockFactory delegate,
        @NotNull IndexProvider indexProvider,
        @NotNull FileFetcher fileFetcher,
        @NotNull SkyBlockDataWriteContract writeContract,
        @NotNull Gson gson,
        @NotNull Path overlayBasePath,
        int max412ImmediateRetries
    ) {
        this.sourceId = sourceId;
        this.models = delegate.getModels();
        this.sources = Concurrent.newMap();
        this.writableSources = Concurrent.newMap();

        for (Class<JpaModel> modelClass : this.models) {
            WritableRemoteJsonSource<?> writable = buildSourceFor(
                modelClass, sourceId, indexProvider, fileFetcher,
                writeContract, gson, overlayBasePath, max412ImmediateRetries
            );
            this.sources.put(modelClass, writable);
            this.writableSources.put(modelClass, writable);
        }

        log.info(
            "RemoteSkyBlockFactory wired {} models from source '{}' with overlayBasePath='{}' (writable write path active)",
            this.models.size(), sourceId, overlayBasePath
        );
    }

    @Override
    public @Nullable Source<?> getDefaultSource() {
        return null;
    }

    @Override
    public @NotNull ConcurrentMap<Class<?>, Consumer<?>> getPeeks() {
        return Concurrent.newUnmodifiableMap();
    }

    /**
     * Builds the per-model source chain:
     * {@code WritableRemoteJsonSource(DiskOverlaySource(RemoteJsonSource(...)))}.
     * The read path traverses the disk overlay then the remote JSON source; the
     * write path bypasses both layers and goes direct to GitHub via
     * {@code WritableRemoteJsonSource.commitBatch()}.
     *
     * @param modelClass the entity type
     * @param sourceId the human-readable source id
     * @param indexProvider the manifest index provider
     * @param fileFetcher the raw-file fetcher
     * @param writeContract the GitHub Contents API write-path contract
     * @param gson the Gson instance for entity list serialization
     * @param overlayBasePath the base directory for overlay file resolution
     * @param max412ImmediateRetries maximum immediate 412 retries per commit tick
     * @param <T> the entity type
     * @return a fully wrapped writable source ready to be registered in {@link #sources}
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <T extends JpaModel> @NotNull WritableRemoteJsonSource<T> buildSourceFor(
        @NotNull Class modelClass,
        @NotNull String sourceId,
        @NotNull IndexProvider indexProvider,
        @NotNull FileFetcher fileFetcher,
        @NotNull SkyBlockDataWriteContract writeContract,
        @NotNull Gson gson,
        @NotNull Path overlayBasePath,
        int max412ImmediateRetries
    ) {
        Table table = (Table) modelClass.getAnnotation(Table.class);

        if (table == null)
            throw new IllegalStateException(
                "Model class '" + modelClass.getName() + "' is missing a @Table annotation - cannot resolve overlay path"
            );

        Path overlayPath = overlayBasePath.resolve(table.name() + OVERLAY_SUFFIX);
        RemoteJsonSource<T> remote = new RemoteJsonSource<>(sourceId, indexProvider, fileFetcher, (Class<T>) modelClass);
        DiskOverlaySource<T> overlay = new DiskOverlaySource<>(remote, overlayPath, (Class<T>) modelClass);
        return new WritableRemoteJsonSource<>(
            overlay,
            writeContract,
            fileFetcher,
            indexProvider,
            gson,
            sourceId,
            (Class<T>) modelClass,
            max412ImmediateRetries
        );
    }

}
