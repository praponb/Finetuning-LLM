from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code")
w_hist = Window.partitionBy("channel")
w3 = Window.partitionBy("promo_code", "channel").orderBy("discount_pct")
w4 = Window.partitionBy("promo_code")
w5 = Window.orderBy(F.col("promo_id").desc_nulls_first())
w6 = Window.orderBy(F.col("promotion_discount_pct").desc_nulls_first())
w7 = Window.partitionBy("channel").orderBy(F.col("discount_pct_cume_dist").desc())

filtered = (
    df_promotions
    .filter((F.col("starts_on") > F.to_date(F.lit("2023-09-05"))) & F.col("channel").isNotNull() & (F.year("starts_on") == 2023))
    .select("*", F.length("channel").alias("channel_length"))
)

matched_keys = (
    filtered
    .select("channel")
    .exceptAll(
        df_promotions
        .filter(F.col("starts_on") > "2023-11-01")
        .select("channel")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

attributed = (
    reconciled
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("e.starts_on") >= F.col("pr.starts_on")) & (F.col("e.starts_on") <= F.date_add(F.col("pr.ends_on"), 3)), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

enriched = (
    attributed
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w)), 4))
    .withColumn("channel_count_promotion_discount_pct", F.count("promotion_discount_pct").over(w_hist))
    .withColumn("discount_pct_cume_dist", F.cume_dist().over(w3))
)

with_windows = (
    enriched
    .select(
        "*",
        F.max("channel_count_promotion_discount_pct").over(w4).alias("promo_code_max_channel_count_promotion_discount_pct"),
        F.cume_dist().over(w5).alias("promo_id_cume_dist"),
        F.percent_rank().over(w6).alias("promotion_discount_pct_percent_rank"),
        F.dense_rank().over(w7).alias("discount_pct_cume_dist_dense_rank"),
    )
)

base = (
    with_windows
    .filter((F.col("ends_on") > F.to_date(F.lit("2023-09-24"))) & F.col("promotion_promo_code").isNotNull())
    .withColumn("channel_promo_code_key", F.concat_ws("-", "channel", "promo_code"))
    .withColumn("channel_prefix", F.substring("channel", 1, 2))
    .withColumn("promotion_promo_code_clean", F.coalesce("promotion_promo_code", F.lit("OTHER")))
)

base_promotions = (
    base
    .filter((F.col("starts_on") >= F.lit("2023-04-14").cast("date")) & F.col("channel").isNotNull() & (F.col("discount_pct") > 0.23))
    .select(
        "id",
        "promo_code",
        "starts_on",
        "discount_pct",
        "channel_length",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        "discount_pct_share",
        "discount_pct_cume_dist",
        "promo_code_max_channel_count_promotion_discount_pct",
        "promo_id_cume_dist",
        "promotion_discount_pct_percent_rank",
        "discount_pct_cume_dist_dense_rank",
        "channel_prefix",
        "promotion_promo_code_clean",
        (F.col("discount_pct_share") + F.col("channel_length")).alias("discount_pct_share_plus_channel_length"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean"),
    )
)

summary = (
    base_promotions
    .groupBy("promo_code")
    .agg(
        F.sort_array(F.collect_set("promotion_promo_code")).alias("promotion_promo_code_list"),
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
        F.count("*").alias("event_count"),
    )
)

result = (
    summary
    .select(
        "promo_code",
        "promo_code_list",
        "num_promo_codes",
        "event_count",
        F.when(F.col("promo_code").rlike("^(SPRING10|VIP)$"), "GROUP_A").when(F.col("promo_code").rlike("^(BUNDLE|WELCOME5)$"), "CORE").otherwise("PRIORITY").alias("promo_code_zone"),
    )
)
