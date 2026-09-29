from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_first())
w_recent = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_last())
w_recent_2 = Window.partitionBy("currency").orderBy(F.col("val").asc_nulls_first())
w5 = Window.orderBy(F.col("currency_max_val").desc_nulls_last())
w6 = Window.orderBy("val_dense_rank")
w_ordered = Window.orderBy(F.col("currency_max_val").desc())

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("val"),
    )
)

enriched = (
    exploded
    .select(
        "id",
        "effective_from",
        F.max("val").over(w_hist).alias("currency_max_val"),
        F.min("val").over(w_hist).alias("currency_min_val"),
        F.dense_rank().over(w2).alias("val_dense_rank"),
        F.dense_rank().over(w_recent).alias("val_dense_rank_2"),
        F.cume_dist().over(w_recent_2).alias("val_cume_dist"),
    )
)

metrics = (
    enriched
    .filter(F.col("val_dense_rank") == 8)
    .select(
        "currency_max_val",
        "currency_min_val",
        "val_dense_rank",
        F.percent_rank().over(w5).alias("currency_max_val_percent_rank"),
        F.rank().over(w6).alias("val_dense_rank_rank"),
        F.rank().over(w_ordered).alias("currency_max_val_rank"),
    )
)

base = (
    metrics
    .filter(F.col("currency_max_val_percent_rank").between(0.27, 0.49))
    .select(
        "currency_max_val",
        "currency_min_val",
        "val_dense_rank",
        "currency_max_val_percent_rank",
        "val_dense_rank_rank",
        "currency_max_val_rank",
        F.coalesce("currency_min_val", F.lit(0)).alias("currency_min_val_filled"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("currency_max_val_rank") >= 7, "LARGE").otherwise("SMALL").alias("currency_max_val_rank_tier"),
    )
    .agg(
        F.percentile_approx("val_dense_rank_rank", 0.9).alias("val_dense_rank_rank_p90"),
        F.round(F.avg("currency_min_val_filled"), 2).alias("avg_currency_min_val_filled"),
    )
    .orderBy("val_dense_rank_rank_p90")
)
