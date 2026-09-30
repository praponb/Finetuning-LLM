from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").asc_nulls_last())
w2 = Window.partitionBy("currency").orderBy(F.col("val").asc_nulls_first())
w3 = Window.partitionBy("currency").orderBy(F.col("val").desc())
w4 = Window.orderBy(F.col("val").desc_nulls_first())
w5 = Window.orderBy(F.col("val").desc_nulls_last())
w6 = Window.orderBy(F.col("val").desc())

flattened = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("val"),
    )
)

cleaned = (
    flattened
    .filter(F.col("effective_from") > F.lit("2024-06-06").cast("date"))
    .withColumn("effective_from_label", F.date_format("effective_from", "yyyy-MM"))
    .withColumn("currency_group", F.when(F.col("currency").isin(["USD", "THB"]), "Core").otherwise("Other"))
)

enriched = (
    cleaned
    .withColumn("val_rank", F.rank().over(w))
    .withColumn("val_cume_dist", F.cume_dist().over(w2))
    .withColumn("val_dense_rank", F.dense_rank().over(w3))
    .withColumn("val_rank_2", F.rank().over(w4))
)

base = (
    enriched
    .filter(F.col("effective_from") > F.lit("2024-07-28").cast("date"))
    .select(
        "id",
        "effective_from",
        "val",
        "effective_from_label",
        "val_rank",
        "val_cume_dist",
        "val_dense_rank",
        F.coalesce("val_cume_dist", F.lit(0)).alias("val_cume_dist_filled"),
        F.round((F.col("val_rank") * 1.15), 2).alias("val_rank_adjusted"),
    )
)

filtered = (
    base
    .filter(F.col("effective_from") >= F.to_date(F.lit("2024-08-29")))
    .select(
        "id",
        "effective_from",
        "val",
        "val_dense_rank",
        "val_cume_dist_filled",
        "val_rank_adjusted",
        F.round("val_rank", 2).alias("val_rank_rounded"),
        (F.col("val_cume_dist") * F.col("val_cume_dist_filled")).alias("val_cume_dist_times_val_cume_dist_filled"),
    )
)

metrics = (
    filtered
    .withColumn("val_dense_rank_2", F.dense_rank().over(w5))
    .withColumn("val_cume_dist", F.cume_dist().over(w6))
)

result = (
    metrics
    .groupBy(F.date_trunc("QUARTER", "effective_from").alias("quarter_start"))
    .agg(
        F.percentile_approx("val_dense_rank", 0.9).alias("val_dense_rank_p90"),
        F.percentile_approx("val_rank_rounded", [0.25, 0.5, 0.75]).alias("val_rank_rounded_quartiles"),
        F.percentile_approx("val_cume_dist_times_val_cume_dist_filled", 0.9).alias("val_cume_dist_times_val_cume_dist_filled_p90"),
        F.count("*").alias("row_count"),
        F.round(F.avg("val_dense_rank_2"), 2).alias("avg_val_dense_rank_2"),
    )
    .orderBy("val_dense_rank_p90")
)
