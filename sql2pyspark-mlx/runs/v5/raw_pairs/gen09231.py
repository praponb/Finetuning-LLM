from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("effective_from_day").asc_nulls_last())
w2 = Window.orderBy(F.col("tier_value").desc_nulls_first())
w_part = Window.orderBy(F.col("tier_value").asc_nulls_first())

flattened = (
    df_price_tiers
    .select("id", "effective_from", F.explode("tier_limits").alias("tier_value"))
)

cleaned = (
    flattened
    .filter(F.col("effective_from") >= F.to_date(F.lit("2024-02-14")))
    .select("*", F.dayofmonth("effective_from").alias("effective_from_day"))
)

metrics = (
    cleaned
    .withColumn("effective_from_day_dense_rank", F.dense_rank().over(w))
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w2))
    .withColumn("tier_value_rank", F.rank().over(w_part))
)

valid = (
    metrics
    .filter(F.col("effective_from") >= F.lit("2024-05-14").cast("date"))
    .withColumn("least_tier_value_rank_effective_from_day", F.least("tier_value_rank", "effective_from_day"))
    .withColumn("effective_from_month_end", F.last_day("effective_from"))
)

result = (
    valid
    .withColumn("tier_value_dense_rank_tier", F.when(F.col("tier_value_dense_rank") >= 7, "HIGH").otherwise("LOW"))
    .rollup("tier_value_dense_rank_tier")
    .agg(
        F.max("tier_value_dense_rank").alias("max_tier_value_dense_rank"),
        F.round(F.avg("effective_from_day"), 2).alias("avg_effective_from_day"),
        F.percentile_approx("tier_value", [0.25, 0.5, 0.75]).alias("tier_value_quartiles"),
        F.percentile_approx("tier_value", [0.25, 0.5, 0.75]).alias("tier_value_quartiles_2"),
        F.count("*").alias("row_count"),
        F.count("*").alias("num_rows"),
        F.grouping_id().alias("grouping_level"),
    )
)
