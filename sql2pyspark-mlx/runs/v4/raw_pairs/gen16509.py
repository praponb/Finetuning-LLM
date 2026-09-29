from pyspark.sql import functions as F

cleaned = (
    df_promotions
    .filter((F.col("ends_on") > F.lit("2023-06-30").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        F.concat_ws(",", "promo_code", "channel").alias("promo_code_channel_key"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_clean"),
    )
)

matched = (
    cleaned
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("store")) == F.col("pr.channel")) & F.col("e.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_window = (
    matched
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & F.col("src.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

matched_promotions = (
    with_window
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("src.ends_on") >= F.col("pr.starts_on")) & (F.col("src.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_3"),
        F.col("pr.promo_code").alias("promotion_promo_code_3"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_3"),
    )
)

grouped = (
    matched_promotions
    .groupBy("channel")
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.count_if(F.col("channel") == "mobile").alias("mobile_events"),
        F.round(F.avg("promotion_discount_pct_2"), 2).alias("avg_promotion_discount_pct_2"),
        F.count("*").alias("row_count"),
        F.countDistinct("channel").alias("distinct_channels"),
    )
)

result = (
    grouped
    .select(
        "channel",
        "channel_list",
        "mobile_events",
        "avg_promotion_discount_pct_2",
        "row_count",
        F.when(F.col("channel").rlike("^(web|store)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("channel_zone"),
    )
    .orderBy(F.col("avg_promotion_discount_pct_2").asc_nulls_last(), "mobile_events")
)
