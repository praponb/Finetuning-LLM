from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.orderBy(F.col("tier_value").desc_nulls_first())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

metrics = (
    flattened
    .filter(F.col("tier_value") >= 45)
    .select(
        "currency",
        "tier_value",
        F.min("tier_value").over(w).alias("plan_min_tier_value"),
        F.dense_rank().over(w2).alias("tier_value_dense_rank"),
        F.sum("tier_value").over(w).alias("plan_sum_tier_value"),
        F.avg("tier_value").over(w).alias("plan_avg_tier_value"),
    )
)

valid = (
    metrics
    .filter(~F.col("currency").isin(["EUR", "THB"]))
    .select("*", F.substring("currency", 1, 4).alias("currency_prefix"))
)

cleaned = (
    valid
    .filter(F.col("tier_value_dense_rank") <= 4)
    .select(
        "tier_value",
        "plan_min_tier_value",
        "tier_value_dense_rank",
        "plan_sum_tier_value",
        "plan_avg_tier_value",
        "currency_prefix",
        F.col("plan_avg_tier_value").cast("double").alias("plan_avg_tier_value_num"),
        F.when(F.col("currency") == "USD", 1).otherwise(0).alias("is_usd"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("tier_value") >= 73, "HIGH").otherwise("LOW").alias("tier_value_tier"),
    )
    .agg(
        F.percentile_approx("plan_sum_tier_value", 0.5).alias("plan_sum_tier_value_p50"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("row_count"),
        F.percentile_approx("tier_value", [0.25, 0.5, 0.75]).alias("tier_value_quartiles"),
    )
)
