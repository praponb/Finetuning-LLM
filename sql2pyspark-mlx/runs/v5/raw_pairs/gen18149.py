from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("val")
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("plan").orderBy(F.col("plan_count_val").desc_nulls_last())
w4 = Window.partitionBy("plan").orderBy(F.col("val_dense_rank").asc_nulls_last())
w5 = Window.partitionBy("plan").orderBy(F.col("plan_stddev_samp_val").desc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("val"),
    )
)

metrics = (
    lines
    .select(
        "plan",
        "effective_from",
        F.dense_rank().over(w).alias("val_dense_rank"),
        F.count("val").over(w2).alias("plan_count_val"),
        F.stddev_samp("val").over(w2).alias("plan_stddev_samp_val"),
    )
)

kept_keys = (
    metrics
    .select("plan")
    .intersectAll(
        df_price_tiers
        .filter(F.col("plan") != "basic")
        .select("plan")
    )
)

verified = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("effective_from") >= F.lit("2024-02-12").cast("date"))
    .select(
        "*",
        F.greatest("plan_stddev_samp_val", "plan_count_val").alias("greatest_plan_stddev_samp_val_plan_count_val"),
    )
)

with_windows = (
    base
    .filter(F.col("greatest_plan_stddev_samp_val_plan_count_val") > 34)
    .select(
        "*",
        F.percent_rank().over(w3).alias("plan_count_val_percent_rank"),
        F.percent_rank().over(w4).alias("val_dense_rank_percent_rank"),
        F.round((F.col("plan_count_val") / F.sum("plan_count_val").over(w2)), 4).alias("plan_count_val_share"),
        F.round((F.col("plan_count_val") / F.sum("plan_count_val").over(w2)), 4).alias("plan_count_val_share_2"),
    )
)

enriched = with_windows.withColumn("plan_stddev_samp_val_rank", F.rank().over(w5))

result = (
    enriched
    .select(
        "plan",
        "effective_from",
        "plan_stddev_samp_val",
        "plan_count_val_percent_rank",
        "val_dense_rank_percent_rank",
        "plan_count_val_share",
        "plan_count_val_share_2",
        "plan_stddev_samp_val_rank",
    )
    .orderBy(F.col("plan_stddev_samp_val").asc_nulls_last())
)
