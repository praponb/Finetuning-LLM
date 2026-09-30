from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")
w2 = Window.partitionBy("channel").orderBy("discount_pct", "id")
w3 = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("promo_code", "channel").orderBy("discount_pct", "id")

valid = (
    df_promotions
    .filter(F.col("starts_on") >= F.lit("2023-05-12").cast("date"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        F.date_add("starts_on", 29).alias("starts_on_plus_29d"),
        F.coalesce("channel", F.lit("DIRECT")).alias("resolved_channel"),
    )
)

scored = (
    valid
    .filter(F.col("channel") != "web")
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w)), 4))
    .withColumn("next_discount_pct", F.lead("discount_pct", 1).over(w2))
)

enriched = (
    scored
    .withColumn("last3_avg_next_discount_pct", F.avg("next_discount_pct").over(w3))
    .withColumn("last3_sum_next_discount_pct", F.sum("next_discount_pct").over(w3))
    .withColumn("last3_max_next_discount_pct", F.max("next_discount_pct").over(w3))
    .withColumn("next_discount_pct_2", F.lead("discount_pct", 1).over(w4))
)

matched_keys = (
    enriched
    .select("channel")
    .subtract(
        df_promotions
        .filter(F.col("discount_pct").between(0.13, 0.29))
        .select("channel")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.col("channel").isNotNull())
    .withColumn("days_starts_on_plus_29d_to_starts_on", F.datediff("starts_on", "starts_on_plus_29d"))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("DIRECT")))
)

scored_promotions = (
    cleaned
    .withColumn("channel_stddev_samp_last3_sum_next_discount_pct", F.stddev_samp("last3_sum_next_discount_pct").over(w))
)

filtered = (
    scored_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "channel",
        "starts_on",
        "starts_on_plus_29d",
        "resolved_channel",
        "discount_pct_share",
        "next_discount_pct",
        "last3_avg_next_discount_pct",
        "last3_sum_next_discount_pct",
        "last3_max_next_discount_pct",
        "next_discount_pct_2",
        "days_starts_on_plus_29d_to_starts_on",
        "channel_clean",
        F.round((F.col("next_discount_pct") / F.col("discount_pct")), 2).alias("next_discount_pct_per_discount_pct"),
        F.when(F.col("discount_pct") >= 0.33, "High").when(F.col("discount_pct") >= 0.14, "Medium").otherwise("Low").alias("discount_pct_band"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_clean_2"),
    )
)

result = (
    filtered
    .select(
        "starts_on_plus_29d",
        "resolved_channel",
        "discount_pct_share",
        "last3_avg_next_discount_pct",
        "last3_max_next_discount_pct",
        "days_starts_on_plus_29d_to_starts_on",
        "channel_clean",
        "next_discount_pct_per_discount_pct",
        "channel_clean_2",
    )
    .orderBy(
        F.col("next_discount_pct_per_discount_pct").asc_nulls_last(),
        F.col("starts_on_plus_29d").desc(),
    )
)
