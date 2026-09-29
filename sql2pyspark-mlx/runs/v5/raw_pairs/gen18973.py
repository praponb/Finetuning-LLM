from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("plan")
w2 = Window.orderBy(F.col("val").asc_nulls_first())
w_running = Window.orderBy(F.col("val").desc())

flattened = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode_outer("tier_prices").alias("val"))
)

metrics = (
    flattened
    .filter(F.col("val").between(46, 55))
    .select(
        "*",
        F.min("val").over(w_recent).alias("plan_min_val"),
        F.rank().over(w2).alias("val_rank"),
        F.count("val").over(w_recent).alias("plan_count_val"),
        F.rank().over(w_running).alias("val_rank_2"),
        F.stddev_samp("val").over(w_recent).alias("plan_stddev_samp_val"),
    )
)

base = (
    metrics
    .filter((F.col("effective_from") >= F.lit("2024-04-25").cast("date")) & (F.col("val_rank") == 6))
    .withColumn("plan_stddev_samp_val_plus_plan_min_val", (F.col("plan_stddev_samp_val") + F.col("plan_min_val")))
)

cleaned = (
    base
    .filter(F.col("effective_from") >= F.to_date(F.lit("2024-01-31")))
    .select(
        "id",
        "val_rank",
        "plan_count_val",
        "val_rank_2",
        "plan_stddev_samp_val",
        "plan_stddev_samp_val_plus_plan_min_val",
        F.col("plan_min_val").cast("string").alias("plan_min_val_str"),
    )
)

base_price_tiers = (
    cleaned
    .filter(F.col("plan_stddev_samp_val") >= 52)
    .select(
        "*",
        F.round("plan_stddev_samp_val_plus_plan_min_val", 0).alias("plan_stddev_samp_val_plus_plan_min_val_rounded"),
        F.round((F.col("val_rank_2") / F.col("val_rank")), 2).alias("val_rank_2_per_val_rank"),
    )
)

result = (
    base_price_tiers
    .select(
        "id",
        "val_rank",
        "plan_stddev_samp_val",
        "plan_stddev_samp_val_plus_plan_min_val",
        "plan_min_val_str",
        "val_rank_2_per_val_rank",
    )
    .orderBy(F.col("val_rank").asc_nulls_last())
)
