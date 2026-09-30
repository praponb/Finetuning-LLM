from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code")
w2 = Window.partitionBy("promotion_promo_code")

remaining_keys = (
    df_promotions
    .select("channel")
    .subtract(
        df_promotions
        .filter(F.col("discount_pct") > 0.14)
        .select("channel")
    )
)

verified = (
    df_promotions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

with_window = (
    verified
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & F.col("a.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_windows = (
    with_window
    .select(
        "*",
        F.stddev_samp("promotion_discount_pct").over(w).alias("channel_stddev_samp_promotion_discount_pct"),
        F.round((F.col("promotion_discount_pct") / F.sum("promotion_discount_pct").over(w2)), 4).alias("promotion_discount_pct_share"),
    )
)

base = (
    with_windows
    .filter(F.col("starts_on") >= F.lit("2023-09-01").cast("date"))
    .select(
        "*",
        (F.col("channel_stddev_samp_promotion_discount_pct") / F.nullif("promotion_discount_pct_share", F.lit(0))).alias("channel_stddev_samp_promotion_discount_pct_per_promotion_discount_pct_share"),
        F.length("channel").alias("channel_length"),
    )
)

base_promotions = (
    base
    .filter((F.col("ends_on") >= F.lit("2024-05-10").cast("date")) & F.col("promotion_promo_code").isNotNull() & (F.col("ends_on") < "2023-06-24"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "promo_id",
        "promotion_discount_pct",
        "channel_stddev_samp_promotion_discount_pct",
        "promotion_discount_pct_share",
        "channel_stddev_samp_promotion_discount_pct_per_promotion_discount_pct_share",
        F.upper("promotion_promo_code").alias("promotion_promo_code_upper"),
        F.date_add("ends_on", 31).alias("ends_on_plus_31d"),
        F.coalesce("channel", F.lit("N/A")).alias("resolved_channel"),
    )
)

result = (
    base_promotions
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "promo_id",
        "promotion_discount_pct_share",
        "channel_stddev_samp_promotion_discount_pct_per_promotion_discount_pct_share",
        "ends_on_plus_31d",
        "resolved_channel",
    )
    .orderBy(F.col("starts_on").desc())
)
