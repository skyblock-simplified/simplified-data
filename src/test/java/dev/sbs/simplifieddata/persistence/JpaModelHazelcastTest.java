package dev.sbs.simplifieddata.persistence;

import dev.sbs.minecraftapi.MinecraftApi;
import dev.sbs.minecraftapi.persistence.model.*;
import dev.sbs.renderer.text.ChatColor;
import dev.simplified.collection.ConcurrentList;
import dev.simplified.persistence.JpaCacheProvider;
import dev.simplified.persistence.Repository;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Drives the production SkyBlock model layer through {@link JpaCacheProvider#HAZELCAST_EMBEDDED}
 * to prove that all four custom Gson types ({@code GsonJsonType}, {@code GsonListType},
 * {@code GsonMapType}, {@code GsonOptionalType}) serialize correctly through the Hazelcast
 * second-level cache.
 *
 * <p>This test class lives in {@code simplified-data}'s test source set rather than
 * {@code minecraft-api}'s because {@code simplified-data} has no {@code TestLifecycleListener}
 * auto-connect, leaving its test JVM as a clean slate for {@link #beforeAll()} to register the
 * sole SkyBlock {@link dev.simplified.persistence.JpaSession} via
 * {@link MinecraftApi#connectSkyBlockSession(JpaCacheProvider)} with
 * {@link JpaCacheProvider#HAZELCAST_EMBEDDED}.</p>
 *
 * <p>Test resource {@code hazelcast.xml} on the classpath bootstraps the in-process Hazelcast
 * member with cluster name {@code skyblock-test} and disabled discovery (port range 5801-5820,
 * intentionally distinct from the production 5701 so the in-process member cannot accidentally
 * join a locally-running production cluster).</p>
 *
 * <p>Test cases mirror {@link dev.sbs.minecraftapi.model.JpaModelTest} from {@code minecraft-api}
 * to exercise the production SkyBlock JSON model corpus end to end against real Hazelcast.</p>
 *
 * <p>Phase 2d (lazy streaming JpaRepository rewrite) re-enables this test. The Phase 2a
 * crash root-caused to {@code JpaRepository.stream()} forcing
 * {@code setCacheable(true).getResultList()} on every query, which routed results through
 * Hibernate's query results region as {@code QueryResultsCacheImpl$CacheItem}
 * (a {@link java.io.Serializable} envelope). Phase 2d switched {@code JpaRepository.stream()}
 * to lazy {@code Query.getResultStream()} and disabled {@code hibernate.cache.use_query_cache}
 * for both Hazelcast providers - the query results region is no longer created or written to,
 * so the {@code ObjectOutputStream} graph walk that crashed on {@link java.util.Optional},
 * {@code Stat$Substitute}, and friends is never entered. The L2 entity cache still populates
 * as a side effect of streaming hydration, so per-id lookups remain cache-fast.</p>
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class JpaModelHazelcastTest {

    @BeforeAll
    static void beforeAll() {
        // Connect the SkyBlock H2 in-memory session backed by the in-process Hazelcast member.
        // Referencing MinecraftApi triggers its static initializer (registers SkyBlockFactory in
        // the service manager) without auto-connecting any session; this explicit call is the
        // sole connect site for the test JVM.
        MinecraftApi.connectSkyBlockSession(JpaCacheProvider.HAZELCAST_EMBEDDED);
    }

    @AfterAll
    static void afterAll() {
        // Symmetric cleanup: shut down the SessionManager so the in-process Hazelcast member
        // and its JpaSession are released. Leaves the JVM clean for any sibling test class.
        MinecraftApi.getSessionManager().shutdown();
    }

    // ---------------------------------------------------------------
    // Leaf models (no FK dependencies)
    // ---------------------------------------------------------------

    @Test
    @Order(1)
    void region_loadsFromJson() {
        Repository<Region> repo = MinecraftApi.getRepository(Region.class);
        ConcurrentList<Region> all = repo.findAll();
        assertThat(all, not(empty()));

        Region hub = repo.findFirst(Region::getId, "HUB").orElseThrow();
        assertThat(hub.getName(), is("Hub"));
        assertThat(hub.getFormat(), is(ChatColor.Legacy.WHITE));
        assertThat(hub.getGameType(), is("SKYBLOCK"));
        assertThat(hub.getMode(), is("HUB"));
    }

    @Test
    @Order(1)
    void statCategory_loadsFromJson() {
        Repository<StatCategory> repo = MinecraftApi.getRepository(StatCategory.class);
        ConcurrentList<StatCategory> all = repo.findAll();
        assertThat(all, not(empty()));

        StatCategory combat = repo.findFirst(StatCategory::getId, "COMBAT").orElseThrow();
        assertThat(combat.getName(), is("Combat"));
        assertThat(combat.getFormat(), is(ChatColor.Legacy.RED));
    }

    @Test
    @Order(1)
    void mobType_loadsFromJson() {
        Repository<MobType> repo = MinecraftApi.getRepository(MobType.class);
        ConcurrentList<MobType> all = repo.findAll();
        assertThat(all, not(empty()));

        MobType undead = repo.findFirst(MobType::getId, "UNDEAD").orElseThrow();
        assertThat(undead.getName(), is("Undead"));
        assertThat(undead.getFormat(), is(ChatColor.Legacy.DARK_GREEN));
    }

    @Test
    @Order(1)
    void itemCategory_loadsFromJson() {
        Repository<ItemCategory> repo = MinecraftApi.getRepository(ItemCategory.class);
        ConcurrentList<ItemCategory> all = repo.findAll();
        assertThat(all, not(empty()));
    }

    @Test
    @Order(1)
    void essence_loadsFromJson() {
        Repository<Essence> repo = MinecraftApi.getRepository(Essence.class);
        ConcurrentList<Essence> all = repo.findAll();
        assertThat(all, not(empty()));
    }

    @Test
    @Order(1)
    void keyword_loadsFromJson() {
        Repository<Keyword> repo = MinecraftApi.getRepository(Keyword.class);
        ConcurrentList<Keyword> all = repo.findAll();
        assertThat(all, not(empty()));
    }

    @Test
    @Order(1)
    void melodySong_loadsFromJson() {
        Repository<MelodySong> repo = MinecraftApi.getRepository(MelodySong.class);
        ConcurrentList<MelodySong> all = repo.findAll();
        assertThat(all, not(empty()));
    }

    @Test
    @Order(1)
    void zodiacEvent_loadsFromJson() {
        Repository<ZodiacEvent> repo = MinecraftApi.getRepository(ZodiacEvent.class);
        ConcurrentList<ZodiacEvent> all = repo.findAll();
        assertThat(all, not(empty()));
    }

    @Test
    @Order(1)
    void power_loadsFromJson() {
        Repository<Power> repo = MinecraftApi.getRepository(Power.class);
        ConcurrentList<Power> all = repo.findAll();
        assertThat(all, not(empty()));
    }

    @Test
    @Order(1)
    void potionGroup_loadsFromJson() {
        Repository<PotionGroup> repo = MinecraftApi.getRepository(PotionGroup.class);
        ConcurrentList<PotionGroup> all = repo.findAll();
        assertThat(all, not(empty()));
    }

    @Test
    @Order(1)
    void bitsItem_loadsFromJson() {
        Repository<BitsItem> repo = MinecraftApi.getRepository(BitsItem.class);
        ConcurrentList<BitsItem> all = repo.findAll();
        assertThat(all, not(empty()));
    }

    @Test
    @Order(1)
    void mayor_loadsFromJson() {
        Repository<Mayor> repo = MinecraftApi.getRepository(Mayor.class);
        ConcurrentList<Mayor> all = repo.findAll();
        assertThat(all, not(empty()));
    }

    @Test
    @Order(1)
    void skill_loadsFromJson() {
        Repository<Skill> repo = MinecraftApi.getRepository(Skill.class);
        ConcurrentList<Skill> all = repo.findAll();
        assertThat(all, not(empty()));
    }

    @Test
    @Order(1)
    void collection_loadsFromJson() {
        Repository<Collection> repo = MinecraftApi.getRepository(Collection.class);
        ConcurrentList<Collection> all = repo.findAll();
        assertThat(all, not(empty()));
    }

    @Test
    @Order(1)
    void potion_loadsFromJson() {
        Repository<Potion> repo = MinecraftApi.getRepository(Potion.class);
        ConcurrentList<Potion> all = repo.findAll();
        assertThat(all, not(empty()));
    }

    @Test
    @Order(1)
    void brew_loadsFromJson() {
        Repository<Brew> repo = MinecraftApi.getRepository(Brew.class);
        ConcurrentList<Brew> all = repo.findAll();
        assertThat(all, not(empty()));
    }

    // ---------------------------------------------------------------
    // Models with FK dependencies (loaded after leaf models)
    // ---------------------------------------------------------------

    @Test
    @Order(2)
    void stat_loadsFromJson_withCategoryFk() {
        Repository<Stat> repo = MinecraftApi.getRepository(Stat.class);
        ConcurrentList<Stat> all = repo.findAll();
        assertThat(all, not(empty()));

        Stat health = repo.findFirst(Stat::getId, "HEALTH").orElseThrow();
        assertThat(health.getName(), is("Health"));
        assertThat(health.getCategoryId(), is("COMBAT"));
        // @ManyToOne FK resolution
        assertThat(health.getCategory(), notNullValue());
        assertThat(health.getCategory().getId(), is("COMBAT"));
    }

    @Test
    @Order(2)
    void zone_loadsFromJson_withRegionFk() {
        Repository<Zone> repo = MinecraftApi.getRepository(Zone.class);
        ConcurrentList<Zone> all = repo.findAll();
        assertThat(all, not(empty()));

        Zone hub = repo.findFirst(Zone::getId, "HUB").orElseThrow();
        assertThat(hub.getRegionId(), is("HUB"));
        // @ManyToOne FK resolution
        assertThat(hub.getRegion(), notNullValue());
        assertThat(hub.getRegion().getId(), is("HUB"));
    }

    @Test
    @Order(2)
    void item_loadsFromJson_withCategoryFk() {
        Repository<Item> repo = MinecraftApi.getRepository(Item.class);
        ConcurrentList<Item> all = repo.findAll();
        assertThat(all, not(empty()));

        // Verify @ManyToOne FK resolution on at least one item
        Item first = all.getFirst();
        assertThat(first.getCategory(), notNullValue());
    }

    @Test
    @Order(2)
    void gemstone_loadsFromJson_withStatFk() {
        Repository<Gemstone> repo = MinecraftApi.getRepository(Gemstone.class);
        ConcurrentList<Gemstone> all = repo.findAll();
        assertThat(all, not(empty()));

        Gemstone amber = repo.findFirst(Gemstone::getId, "AMBER").orElseThrow();
        // @ManyToOne FK resolution
        assertThat(amber.getStat(), notNullValue());
        assertThat(amber.getStat().getId(), is("MINING_SPEED"));
    }

    @Test
    @Order(2)
    void bestiaryCategory_loadsFromJson() {
        Repository<BestiaryCategory> repo = MinecraftApi.getRepository(BestiaryCategory.class);
        ConcurrentList<BestiaryCategory> all = repo.findAll();
        assertThat(all, not(empty()));
    }

    @Test
    @Order(2)
    void bestiarySubcategory_loadsFromJson_withCategoryFk() {
        Repository<BestiarySubcategory> repo = MinecraftApi.getRepository(BestiarySubcategory.class);
        ConcurrentList<BestiarySubcategory> all = repo.findAll();
        assertThat(all, not(empty()));

        // @ManyToOne FK resolution
        BestiarySubcategory first = all.getFirst();
        assertThat(first.getCategory(), notNullValue());
    }

    @Test
    @Order(2)
    void pet_loadsFromJson_withSkillFk() {
        Repository<Pet> repo = MinecraftApi.getRepository(Pet.class);
        ConcurrentList<Pet> all = repo.findAll();
        assertThat(all, not(empty()));

        Pet ammonite = repo.findFirst(Pet::getId, "AMMONITE").orElseThrow();
        // @ManyToOne FK resolution
        assertThat(ammonite.getSkill(), notNullValue());
        assertThat(ammonite.getSkill().getId(), is("FISHING"));
    }

    @Test
    @Order(2)
    void slayer_loadsFromJson_withMobTypeFk() {
        Repository<Slayer> repo = MinecraftApi.getRepository(Slayer.class);
        ConcurrentList<Slayer> all = repo.findAll();
        assertThat(all, not(empty()));

        Slayer blaze = repo.findFirst(Slayer::getId, "BLAZE").orElseThrow();
        // @ManyToOne FK resolution
        assertThat(blaze.getMobType(), notNullValue());
        assertThat(blaze.getMobType().getId(), is("INFERNAL"));
    }

    @Test
    @Order(2)
    void fairySoul_loadsFromJson() {
        Repository<FairySoul> repo = MinecraftApi.getRepository(FairySoul.class);
        ConcurrentList<FairySoul> all = repo.findAll();
        assertThat(all, notNullValue()); // JSON file is currently empty
    }

    @Test
    @Order(2)
    void minion_loadsFromJson_withCollectionFk() {
        Repository<Minion> repo = MinecraftApi.getRepository(Minion.class);
        ConcurrentList<Minion> all = repo.findAll();
        assertThat(all, not(empty()));

        // @ManyToOne FK resolution
        Minion first = all.getFirst();
        assertThat(first.getCollection(), notNullValue());
    }

    @Test
    @Order(2)
    void shopPerk_loadsFromJson_withRegionsForeignIds() {
        Repository<ShopPerk> repo = MinecraftApi.getRepository(ShopPerk.class);
        ConcurrentList<ShopPerk> all = repo.findAll();
        assertThat(all, not(empty()));

        ShopPerk catacombsLuck = repo.findFirst(ShopPerk::getId, "CATACOMBS_BOSS_LUCK").orElseThrow();
        assertThat(catacombsLuck.getRegionIds(), hasItem("THE_CATACOMBS"));
        // @ForeignIds resolution
        assertThat(catacombsLuck.getRegions(), not(empty()));
        assertThat(catacombsLuck.getRegions().getFirst().getId(), is("THE_CATACOMBS"));
    }

    // ---------------------------------------------------------------
    // Models with deeper FK chains
    // ---------------------------------------------------------------

    @Test
    @Order(3)
    void accessory_loadsFromJson_withItemFk() {
        Repository<Accessory> repo = MinecraftApi.getRepository(Accessory.class);
        ConcurrentList<Accessory> all = repo.findAll();
        assertThat(all, not(empty()));

        // @ManyToOne FK resolution (id -> Item.id)
        Accessory first = all.getFirst();
        assertThat(first.getItem(), notNullValue());
        assertThat(first.getItem().getId(), is(first.getId()));
    }

    @Test
    @Order(3)
    void enchantment_loadsFromJson_withForeignIds() {
        Repository<Enchantment> repo = MinecraftApi.getRepository(Enchantment.class);
        ConcurrentList<Enchantment> all = repo.findAll();
        assertThat(all, not(empty()));

        // @ForeignIds("categoryIds") resolution
        Enchantment absorb = repo.findFirst(Enchantment::getId, "ABSORB").orElseThrow();
        assertThat(absorb.getCategoryIds(), hasItem("AXE"));
        assertThat(absorb.getCategories(), not(empty()));
        assertThat(absorb.getCategories().getFirst().getId(), is("AXE"));

        // @ForeignIds("mobTypeIds") resolution
        Enchantment cubism = repo.findFirst(Enchantment::getId, "CUBISM").orElseThrow();
        assertThat(cubism.getMobTypeIds(), hasItem("CUBIC"));
        assertThat(cubism.getMobTypes(), not(empty()));
        assertThat(cubism.getMobTypes().getFirst().getId(), is("CUBIC"));

        // @ForeignIds("itemIds") resolution
        Enchantment jerry = repo.findFirst(Enchantment::getId, "ULTIMATE_JERRY").orElseThrow();
        assertThat(jerry.getItemIds(), not(empty()));
        assertThat(jerry.getItems(), not(empty()));
    }

    @Test
    @Order(3)
    void reforge_loadsFromJson_withForeignIds() {
        Repository<Reforge> repo = MinecraftApi.getRepository(Reforge.class);
        ConcurrentList<Reforge> all = repo.findAll();
        assertThat(all, not(empty()));

        // @ForeignIds("categoryIds") resolution
        Reforge fair = repo.findFirst(Reforge::getId, "FAIR").orElseThrow();
        assertThat(fair.getCategoryIds(), not(empty()));
        assertThat(fair.getCategories(), not(empty()));

        // @ForeignIds("itemIds") resolution
        Reforge warped = repo.findFirst(Reforge::getId, "WARPED").orElseThrow();
        assertThat(warped.getItemIds(), not(empty()));
        assertThat(warped.getItems(), not(empty()));

        // Nullable @ManyToOne FK (stone) - absent
        assertThat(fair.getStone().isPresent(), is(false));
    }

    @Test
    @Order(3)
    void mixin_loadsFromJson_withFkAndForeignIds() {
        Repository<Mixin> repo = MinecraftApi.getRepository(Mixin.class);
        ConcurrentList<Mixin> all = repo.findAll();
        assertThat(all, not(empty()));

        // @ManyToOne FK resolution (item_id -> Item.id)
        Mixin first = all.getFirst();
        assertThat(first.getItem(), notNullValue());

        // @ForeignIds("regionIds") resolution on a mixin with regions
        Mixin deepterror = repo.findFirst(Mixin::getId, "DEEPTERROR_MIXIN").orElseThrow();
        assertThat(deepterror.getRegionIds(), not(empty()));
        assertThat(deepterror.getRegions(), not(empty()));
    }

    @Test
    @Order(3)
    void bestiaryFamily_loadsFromJson_withFkAndForeignIds() {
        Repository<BestiaryFamily> repo = MinecraftApi.getRepository(BestiaryFamily.class);
        ConcurrentList<BestiaryFamily> all = repo.findAll();
        assertThat(all, not(empty()));

        // @ManyToOne FK resolution (category_id -> BestiaryCategory.id)
        BestiaryFamily bat = repo.findFirst(BestiaryFamily::getId, "ISLAND_BAT").orElseThrow();
        assertThat(bat.getCategory(), notNullValue());
        assertThat(bat.getCategory().getId(), is("YOUR_ISLAND"));

        // @ForeignIds("mobTypeIds") resolution
        assertThat(bat.getMobTypeIds(), hasItems("ANIMAL", "AIRBORNE"));
        assertThat(bat.getMobTypes(), not(empty()));
        assertThat(bat.getMobTypes().size(), is(bat.getMobTypeIds().size()));

        // Nullable @ManyToOne FK (subcategory) - present
        BestiaryFamily miner = repo.findFirst(BestiaryFamily::getId, "ABYSSAL_MINER").orElseThrow();
        assertThat(miner.getSubcategory().isPresent(), is(true));
        assertThat(miner.getSubcategory().get().getId(), is("FISHING"));

        // Nullable @ManyToOne FK (subcategory) - absent
        assertThat(bat.getSubcategory().isPresent(), is(false));
    }

}
