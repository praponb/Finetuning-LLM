from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy(F.col("tier_value").asc_nulls_last())
w2 = Window.orderBy("tier_value")
w3 = Window.orderBy(F.col("tier_value").desc_nulls_first())
w_running = Window.orderBy(F.col("days_since_effective_from").asc_nulls_last())
w5 = Window.orderBy(F.col("tier_value_dense_rank_2").asc_nulls_last())
w6 = Window.orderBy(F.col("tier_value_rank").desc())

flattened = (
    df_price_tiers
    .select("id", "effective_from", F.explode("discounts").alias("tier_value"))
)

cleaned = (
    flattened
    .filter((F.col("effective_from") >= F.lit("2024-06-02").cast("date")) & (F.col("tier_value") >= 54))
    .select(
        "*",
        F.datediff(F.current_date(), "effective_from").alias("days_since_effective_from"),
    )
)

scored = (
    cleaned
    .withColumn("tier_value_rank", F.rank().over(w_ordered))
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w2))
    .withColumn("tier_value_dense_rank_2", F.dense_rank().over(w3))
)

with_windows = (
    scored
    .select(
        "*",
        F.percent_rank().over(w_running).alias("days_since_effective_from_percent_rank"),
        F.cume_dist().over(w5).alias("tier_value_dense_rank_2_cume_dist"),
        F.cume_dist().over(w_running).alias("days_since_effective_from_cume_dist"),
        F.percent_rank().over(w6).alias("tier_value_rank_percent_rank"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("tier_value_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("tier_value_rank_tier"),
        F.date_trunc("MONTH", "effective_from").alias("activity_month"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("days_since_effective_from_cume_dist", [0.25, 0.5, 0.75]).alias("days_since_effective_from_cume_dist_quartiles"),
    )
    .orderBy(F.col("tier_value_rank_tier").desc())
)
