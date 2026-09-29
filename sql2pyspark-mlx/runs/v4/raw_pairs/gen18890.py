from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("val")
w2 = Window.partitionBy("currency").orderBy(F.col("val").asc_nulls_first())
w3 = Window.partitionBy("currency").orderBy(F.col("val_percent_rank").asc_nulls_first())
w4 = Window.partitionBy("currency")
w5 = Window.partitionBy("currency").orderBy(F.col("val_percent_rank").asc_nulls_last())
w6 = Window.partitionBy("currency").orderBy(F.col("val_percent_rank").desc_nulls_last())
w7 = Window.partitionBy("currency").orderBy(F.col("val_rank_share").desc_nulls_last())
w8 = Window.orderBy(F.col("val_percent_rank").asc_nulls_first())

flattened = df_price_tiers.select("id", "currency", F.explode("discounts").alias("val"))

enriched = (
    flattened
    .withColumn("val_rank", F.rank().over(w))
    .withColumn("val_percent_rank", F.percent_rank().over(w2))
)

with_windows = (
    enriched
    .withColumn("val_percent_rank_cume_dist", F.cume_dist().over(w3))
    .withColumn("val_rank_share", F.round((F.col("val_rank") / F.sum("val_rank").over(w4)), 4))
    .withColumn("currency_avg_val", F.avg("val").over(w4))
    .withColumn("currency_sum_val", F.sum("val").over(w4))
    .withColumn("val_percent_rank_cume_dist_2", F.cume_dist().over(w5))
)

enriched_price_tiers = (
    with_windows
    .withColumn("val_percent_rank_cume_dist_3", F.cume_dist().over(w6))
    .withColumn("currency_min_currency_sum_val", F.min("currency_sum_val").over(w4))
)

with_windows_price_tiers = (
    enriched_price_tiers
    .withColumn("val_rank_share_rank", F.rank().over(w7))
    .withColumn("val_percent_rank_rank", F.rank().over(w8))
)

valid = (
    with_windows_price_tiers
    .filter(F.col("currency") == "USD")
    .select(
        "val_rank",
        "val_percent_rank",
        "val_rank_share",
        "currency_avg_val",
        "currency_sum_val",
        "val_percent_rank_cume_dist_2",
        "currency_min_currency_sum_val",
        "val_rank_share_rank",
        "val_percent_rank_rank",
        (F.col("val_rank_share") / F.nullif("currency_avg_val", F.lit(0))).alias("val_rank_share_per_currency_avg_val"),
    )
)

result = (
    valid
    .withColumn("currency_min_currency_sum_val_tier", F.when(F.col("currency_min_currency_sum_val") >= 327, "PREMIUM").otherwise("STANDARD"))
    .withColumn("val_percent_rank_rank_tier", F.when(F.col("val_percent_rank_rank") >= 9, "LARGE").otherwise("SMALL"))
    .rollup("currency_min_currency_sum_val_tier", "val_percent_rank_rank_tier")
    .agg(
        F.max("val_percent_rank_cume_dist_2").alias("max_val_percent_rank_cume_dist_2"),
        F.percentile_approx("val_rank_share", 0.95).alias("val_rank_share_p95"),
        F.count("*").alias("row_count"),
        F.grouping("currency_min_currency_sum_val_tier").alias("grouping_level"),
    )
    .orderBy("val_percent_rank_rank_tier")
)
