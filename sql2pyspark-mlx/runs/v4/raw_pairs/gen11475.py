from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount_pct_rounded").desc_nulls_first(), "id")
w2 = Window.partitionBy("channel")

base = (
    df_promotions
    .filter((F.col("starts_on") >= F.lit("2023-10-07").cast("date")) & (F.col("promo_code") != "VIP"))
    .withColumn("discount_pct_rounded", F.round("discount_pct", 2))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("N/A")))
)

deduped = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

matched_keys = (
    deduped
    .select("channel")
    .intersect(
        df_promotions
        .filter(F.year("starts_on") >= 2025)
        .select("channel")
    )
)

reconciled = (
    deduped
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .withColumn("channel_min_discount_pct_rounded", F.min("discount_pct_rounded").over(w2))
    .withColumn("channel_min_discount_pct_rounded_2", F.min("discount_pct_rounded").over(w2))
)

cleaned = (
    with_windows
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        "resolved_channel",
        "channel_min_discount_pct_rounded",
        "channel_min_discount_pct_rounded_2",
        F.round((F.col("channel_min_discount_pct_rounded") * 1.15), 2).alias("adjusted_channel_min_discount_pct_rounded"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "starts_on",
        "ends_on",
        "resolved_channel",
        "channel_min_discount_pct_rounded",
        "adjusted_channel_min_discount_pct_rounded",
    )
)
