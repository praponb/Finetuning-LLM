from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").desc())
w2 = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_last())
w3 = Window.partitionBy("currency")
w4 = Window.orderBy("effective_from_month")
w5 = Window.orderBy(F.col("val_percent_rank").desc())
w6 = Window.orderBy(F.col("effective_from_month").desc_nulls_first())

flattened = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("tier_limits").alias("val"))
)

filtered = (
    flattened
    .filter(F.col("effective_from") >= F.lit("2024-06-09").cast("date"))
    .withColumn("effective_from_month", F.month("effective_from"))
)

enriched = (
    filtered
    .select(
        "id",
        "effective_from",
        "val",
        "effective_from_month",
        F.percent_rank().over(w).alias("val_percent_rank"),
        F.percent_rank().over(w2).alias("val_percent_rank_2"),
        F.stddev_samp("effective_from_month").over(w3).alias("currency_stddev_samp_effective_from_month"),
        F.percent_rank().over(w2).alias("val_percent_rank_3"),
    )
)

with_windows = (
    enriched
    .withColumn("effective_from_month_percent_rank", F.percent_rank().over(w4))
    .withColumn("val_percent_rank_cume_dist", F.cume_dist().over(w5))
    .withColumn("effective_from_month_dense_rank", F.dense_rank().over(w6))
)

matched_keys = (
    with_windows
    .select("id")
    .intersectAll(
        df_price_tiers
        .filter(F.col("currency") != "USD")
        .select("id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "effective_from",
        "effective_from_month",
        "val_percent_rank",
        "currency_stddev_samp_effective_from_month",
        "val_percent_rank_3",
        "effective_from_month_percent_rank",
        "val_percent_rank_cume_dist",
    )
    .orderBy(F.col("currency_stddev_samp_effective_from_month").desc_nulls_last())
)
