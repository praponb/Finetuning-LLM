from pyspark.sql import functions as F

base = (
    df_promotions
    .filter(F.col("starts_on") >= "2025-08-30")
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        F.date_format("ends_on", "yyyy-MM").alias("ends_on_label"),
    )
)

remaining_keys = (
    base
    .select("id")
    .intersectAll(
        df_promotions
        .filter(F.col("ends_on") > "2024-05-01")
        .select("id")
    )
)

reconciled = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

attributed = (
    reconciled
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("s.starts_on") >= F.col("pr.starts_on")) & (F.col("s.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct"),
    )
)

base_promotions = (
    attributed
    .filter((F.col("ends_on") > F.lit("2023-06-25").cast("date")) & F.col("channel").isNotNull())
    .withColumn("starts_on_plus_5d", F.date_add("starts_on", 5))
    .withColumn("ends_on_plus_months", F.add_months("ends_on", 6))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("DIRECT")))
)

remaining_keys_2 = (
    base_promotions
    .select("id", "promo_code")
    .intersectAll(
        df_promotions
        .filter(F.col("discount_pct") > 0.17)
        .select("id", "promo_code")
    )
)

verified = (
    base_promotions
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("discount_pct") >= 0.33, "PREMIUM").otherwise("STANDARD").alias("discount_pct_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.countDistinct("channel").alias("distinct_channels"),
    )
    .orderBy(F.col("discount_pct_tier").desc_nulls_last())
)
