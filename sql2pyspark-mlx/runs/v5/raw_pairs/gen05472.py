from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("plan")
w_recent = Window.orderBy(F.col("days_since_effective_from").desc())
w3 = Window.partitionBy("plan").orderBy("days_since_effective_from_rank")
w_ordered = Window.partitionBy("plan").orderBy(F.col("plan_sum_plan_stddev_samp_days_since_effective_from_2").desc_nulls_last())
w5 = Window.partitionBy("plan").orderBy(F.col("days_since_effective_from_dense_rank").asc_nulls_first())
w6 = Window.orderBy(F.col("days_since_effective_from").desc_nulls_first())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("val"),
    )
)

cleaned = (
    flattened
    .filter(F.col("currency") == "USD")
    .select(
        "id",
        "plan",
        "effective_from",
        F.datediff(F.current_date(), "effective_from").alias("days_since_effective_from"),
    )
)

enriched = (
    cleaned
    .filter(F.col("effective_from") >= "2025-07-01")
    .withColumn("plan_stddev_samp_days_since_effective_from", F.stddev_samp("days_since_effective_from").over(w_part))
    .withColumn("plan_stddev_samp_days_since_effective_from_2", F.stddev_samp("days_since_effective_from").over(w_part))
    .withColumn("days_since_effective_from_rank", F.rank().over(w_recent))
)

scored = (
    enriched
    .withColumn("days_since_effective_from_rank_rank", F.rank().over(w3))
    .withColumn("days_since_effective_from_dense_rank", F.dense_rank().over(w_recent))
    .withColumn("plan_sum_plan_stddev_samp_days_since_effective_from_2", F.sum("plan_stddev_samp_days_since_effective_from_2").over(w_part))
)

metrics = (
    scored
    .select(
        "id",
        "effective_from",
        "days_since_effective_from",
        "days_since_effective_from_dense_rank",
        F.cume_dist().over(w_ordered).alias("plan_sum_plan_stddev_samp_days_since_effective_from_2_cume_dist"),
        F.percent_rank().over(w5).alias("days_since_effective_from_dense_rank_percent_rank"),
        F.rank().over(w6).alias("days_since_effective_from_rank_2"),
    )
)

base = (
    metrics
    .filter(F.col("effective_from") >= F.to_date(F.lit("2024-05-31")))
    .select(
        "id",
        "effective_from",
        "days_since_effective_from_dense_rank",
        "plan_sum_plan_stddev_samp_days_since_effective_from_2_cume_dist",
        "days_since_effective_from_dense_rank_percent_rank",
        "days_since_effective_from_rank_2",
        F.ceil("plan_sum_plan_stddev_samp_days_since_effective_from_2_cume_dist").alias("plan_sum_plan_stddev_samp_days_since_effective_from_2_cume_dist_whole"),
        F.round((F.col("days_since_effective_from_rank_2") / F.col("days_since_effective_from_dense_rank")), 2).alias("days_since_effective_from_rank_2_per_days_since_effective_from_dense_rank"),
    )
)

result = (
    base
    .select(
        "id",
        "effective_from",
        "days_since_effective_from_dense_rank",
        "plan_sum_plan_stddev_samp_days_since_effective_from_2_cume_dist",
        "plan_sum_plan_stddev_samp_days_since_effective_from_2_cume_dist_whole",
        "days_since_effective_from_rank_2_per_days_since_effective_from_dense_rank",
    )
    .orderBy(F.col("days_since_effective_from_dense_rank").desc())
)
