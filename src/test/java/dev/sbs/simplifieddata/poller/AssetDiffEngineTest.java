package dev.sbs.simplifieddata.poller;

import com.google.gson.Gson;
import dev.sbs.minecraftapi.MinecraftApi;
import dev.simplified.collection.Concurrent;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.asset.ExternalAssetEntryState;
import dev.simplified.persistence.source.ManifestIndex;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Unit tests for {@link AssetDiffEngine}.
 *
 * <p>Hand-crafts {@link ManifestIndex} and {@link ExternalAssetEntryState} fixtures via the
 * shared {@link MinecraftApi#getGson()} instance and asserts the three output lists on
 * every classification combination. No Spring, no database - the engine is a pure function
 * and the tests run in a fraction of a millisecond each.
 */
class AssetDiffEngineTest {

    private static final @NotNull Gson GSON = MinecraftApi.getGson();

    @Test
    @DisplayName("empty manifest against empty state yields empty diff")
    void emptyEmpty() {
        AssetDiff diff = AssetDiffEngine.compute(emptyManifest(), Concurrent.newList());

        assertThat(diff.isEmpty(), is(true));
        assertThat(diff.getAdded().size(), equalTo(0));
        assertThat(diff.getChanged().size(), equalTo(0));
        assertThat(diff.getRemoved().size(), equalTo(0));
    }

    @Test
    @DisplayName("manifest with 2 entries against empty state classifies both as added")
    void allNew() {
        AssetDiff diff = AssetDiffEngine.compute(
            manifest(
                """
                    {"path":"data/v1/items/items.json","category":"items","table_name":"item","model_class":"dev.test.Item","content_sha256":"sha-items","bytes":10,"has_extra":false}
                    """,
                """
                    {"path":"data/v1/mobs/mobs.json","category":"mobs","table_name":"mob","model_class":"dev.test.Mob","content_sha256":"sha-mobs","bytes":20,"has_extra":false}
                    """
            ),
            Concurrent.newList()
        );

        assertThat(diff.getAdded().size(), equalTo(2));
        assertThat(diff.getChanged().size(), equalTo(0));
        assertThat(diff.getRemoved().size(), equalTo(0));
    }

    @Test
    @DisplayName("matching sha pair yields no-op (all three lists empty)")
    void noOp() {
        AssetDiff diff = AssetDiffEngine.compute(
            manifest(
                """
                    {"path":"data/v1/items/items.json","category":"items","table_name":"item","model_class":"dev.test.Item","content_sha256":"sha-items","bytes":10,"has_extra":false}
                    """
            ),
            Concurrent.newList(state("data/v1/items/items.json", "sha-items"))
        );

        assertThat(diff.isEmpty(), is(true));
    }

    @Test
    @DisplayName("path present on both sides with sha mismatch yields one changed entry")
    void changedDetected() {
        AssetDiff diff = AssetDiffEngine.compute(
            manifest(
                """
                    {"path":"data/v1/items/items.json","category":"items","table_name":"item","model_class":"dev.test.Item","content_sha256":"sha-new","bytes":10,"has_extra":false}
                    """
            ),
            Concurrent.newList(state("data/v1/items/items.json", "sha-old"))
        );

        assertThat(diff.getAdded().size(), equalTo(0));
        assertThat(diff.getChanged().size(), equalTo(1));
        assertThat(diff.getRemoved().size(), equalTo(0));
        assertThat(diff.getChanged().get(0).getCurrent().getPath(), equalTo("data/v1/items/items.json"));
        assertThat(diff.getChanged().get(0).getCurrent().getContentSha256(), equalTo("sha-new"));
        assertThat(diff.getChanged().get(0).getPrevious().getEntrySha256(), equalTo("sha-old"));
    }

    @Test
    @DisplayName("persisted entry not in manifest is classified as removed")
    void removedDetected() {
        AssetDiff diff = AssetDiffEngine.compute(
            emptyManifest(),
            Concurrent.newList(state("data/v1/items/items.json", "sha-items"))
        );

        assertThat(diff.getAdded().size(), equalTo(0));
        assertThat(diff.getChanged().size(), equalTo(0));
        assertThat(diff.getRemoved().size(), equalTo(1));
        assertThat(diff.getRemoved().get(0).getEntryPath(), equalTo("data/v1/items/items.json"));
    }

    @Test
    @DisplayName("mixed diff with one added, one changed, one removed, one no-op")
    void mixedDiff() {
        AssetDiff diff = AssetDiffEngine.compute(
            manifest(
                """
                    {"path":"data/v1/items/items.json","category":"items","table_name":"item","model_class":"dev.test.Item","content_sha256":"sha-unchanged","bytes":10,"has_extra":false}
                    """,
                """
                    {"path":"data/v1/mobs/mobs.json","category":"mobs","table_name":"mob","model_class":"dev.test.Mob","content_sha256":"sha-new","bytes":20,"has_extra":false}
                    """,
                """
                    {"path":"data/v1/world/regions.json","category":"world","table_name":"region","model_class":"dev.test.Region","content_sha256":"sha-fresh","bytes":30,"has_extra":false}
                    """
            ),
            Concurrent.newList(
                state("data/v1/items/items.json", "sha-unchanged"),
                state("data/v1/mobs/mobs.json", "sha-old"),
                state("data/v1/modifiers/reforges.json", "sha-gone")
            )
        );

        assertThat(diff.getAdded().size(), equalTo(1));
        assertThat(diff.getAdded().get(0).getPath(), equalTo("data/v1/world/regions.json"));
        assertThat(diff.getChanged().size(), equalTo(1));
        assertThat(diff.getChanged().get(0).getCurrent().getPath(), equalTo("data/v1/mobs/mobs.json"));
        assertThat(diff.getRemoved().size(), equalTo(1));
        assertThat(diff.getRemoved().get(0).getEntryPath(), equalTo("data/v1/modifiers/reforges.json"));
    }

    @Test
    @DisplayName("isEmpty returns true on the empty sentinel and false on a populated diff")
    void isEmptySentinel() {
        AssetDiff empty = AssetDiffEngine.compute(emptyManifest(), Concurrent.newList());
        AssetDiff populated = AssetDiffEngine.compute(
            emptyManifest(),
            Concurrent.newList(state("data/v1/items/items.json", "sha-items"))
        );

        assertThat(empty.isEmpty(), is(true));
        assertThat(populated.isEmpty(), is(false));
    }

    // --- helpers --- //

    private static @NotNull ManifestIndex emptyManifest() {
        return ManifestIndex.empty();
    }

    private static @NotNull ManifestIndex manifest(@NotNull String... entryJsons) {
        StringBuilder builder = new StringBuilder("""
            {
              "version": 1,
              "generated_at": "2026-04-07T00:00:00Z",
              "commit_sha": "abc",
              "count": %d,
              "files": [
            """.formatted(entryJsons.length));

        for (int i = 0; i < entryJsons.length; i++) {
            builder.append(entryJsons[i].strip());

            if (i < entryJsons.length - 1)
                builder.append(',');
        }

        builder.append("]}");
        return GSON.fromJson(builder.toString(), ManifestIndex.class);
    }

    private static @NotNull ExternalAssetEntryState state(@NotNull String path, @NotNull String sha) {
        ExternalAssetEntryState row = new ExternalAssetEntryState();
        row.setSourceId("skyblock-data");
        row.setEntryPath(path);
        row.setEntrySha256(sha);
        row.setLastSeenAt(Instant.EPOCH);
        return row;
    }

    @SuppressWarnings("unused")
    private static @NotNull ConcurrentList<ExternalAssetEntryState> states(@NotNull ExternalAssetEntryState... rows) {
        ConcurrentList<ExternalAssetEntryState> list = Concurrent.newList();
        for (ExternalAssetEntryState row : rows)
            list.add(row);
        return list;
    }

}
