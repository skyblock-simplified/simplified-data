package dev.sbs.simplifieddata.persistence;

import dev.sbs.minecraftapi.MinecraftApi;
import dev.sbs.minecraftapi.persistence.SkyBlockFactory;
import dev.sbs.minecraftapi.persistence.model.Accessory;
import dev.sbs.minecraftapi.persistence.model.Item;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.JpaModel;
import dev.simplified.persistence.exception.JpaException;
import dev.simplified.persistence.source.DiskOverlaySource;
import dev.simplified.persistence.source.FileFetcher;
import dev.simplified.persistence.source.IndexProvider;
import dev.simplified.persistence.source.RemoteJsonSource;
import dev.simplified.persistence.source.Source;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link RemoteSkyBlockFactory}. Does NOT boot a Spring context, does NOT
 * require a live Hazelcast cluster, and does NOT touch the network - the GitHub
 * {@link IndexProvider} and {@link FileFetcher} collaborators are hand-rolled stubs that
 * throw on call. Model enumeration is driven by the real {@link SkyBlockFactory} registered
 * in {@link MinecraftApi}.
 */
final class RemoteSkyBlockFactoryTest {

    /** Dummy overlay path - points at a directory that never exists so DiskOverlaySource falls through. */
    private static final @NotNull Path OVERLAY_BASE = Path.of("target/test-overlay-does-not-exist");

    /** Index provider stub that throws if invoked - construction never calls load. */
    private static final @NotNull IndexProvider THROWING_INDEX = () -> {
        throw new JpaException("stub should not be invoked during wiring");
    };

    /** File fetcher stub that throws if invoked - construction never calls fetch. */
    private static final @NotNull FileFetcher THROWING_FETCHER = path -> {
        throw new JpaException("stub should not be invoked during wiring");
    };

    @Test
    void wiresEveryModelWithDiskOverlaySourceWrappingRemoteJsonSource() {
        SkyBlockFactory delegate = MinecraftApi.getSkyBlockFactory();
        RemoteSkyBlockFactory factory = new RemoteSkyBlockFactory(
            "skyblock-data", delegate, THROWING_INDEX, THROWING_FETCHER, OVERLAY_BASE
        );

        assertThat(factory.getModels().size(), equalTo(delegate.getModels().size()));
        assertThat(factory.getModels().size(), greaterThanOrEqualTo(41));
        assertThat(factory.getDefaultSource(), nullValue());
        assertThat(factory.getPeeks().isEmpty(), equalTo(true));

        for (Class<JpaModel> modelClass : delegate.getModels()) {
            Source<?> wired = factory.getSources().get(modelClass);
            assertThat("missing source for " + modelClass.getName(), wired, notNullValue());
            assertThat(wired, instanceOf(DiskOverlaySource.class));

            DiskOverlaySource<?> overlay = (DiskOverlaySource<?>) wired;
            assertThat(overlay.getInner(), instanceOf(RemoteJsonSource.class));
            assertThat(overlay.getModelClass(), equalTo((Class) modelClass));

            RemoteJsonSource<?> remote = (RemoteJsonSource<?>) overlay.getInner();
            assertThat(remote.getSourceId(), equalTo("skyblock-data"));
            assertThat(remote.getModelClass(), equalTo((Class) modelClass));
        }
    }

    @Test
    void keyedMapContainsKnownModelClasses() {
        SkyBlockFactory delegate = MinecraftApi.getSkyBlockFactory();
        RemoteSkyBlockFactory factory = new RemoteSkyBlockFactory(
            "skyblock-data", delegate, THROWING_INDEX, THROWING_FETCHER, OVERLAY_BASE
        );

        assertThat(factory.getSources(), hasKey(Item.class));
        assertThat(factory.getSources(), hasKey(Accessory.class));
    }

    @Test
    void rejectsModelClassWithoutTableAnnotation() {
        SkyBlockFactory delegate = new SkyBlockFactory() {
            @Override
            public @NotNull ConcurrentList<Class<JpaModel>> getModels() {
                ConcurrentList<Class<JpaModel>> single = Concurrent.newList();
                @SuppressWarnings("unchecked")
                Class<JpaModel> unannotated = (Class<JpaModel>) (Class<?>) UnannotatedModel.class;
                single.add(unannotated);
                return single;
            }
        };

        IllegalStateException ex = assertThrows(
            IllegalStateException.class,
            () -> new RemoteSkyBlockFactory("skyblock-data", delegate, THROWING_INDEX, THROWING_FETCHER, OVERLAY_BASE)
        );

        assertThat(ex.getMessage(), allOf(containsString("@Table"), containsString(UnannotatedModel.class.getName())));
    }

    /** Minimal no-@Table JpaModel stand-in for the rejection test. */
    @jakarta.persistence.Entity
    static final class UnannotatedModel implements JpaModel {
    }

}
