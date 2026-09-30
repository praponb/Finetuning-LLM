from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

result = (
    lines
    .groupBy("plan")
    .agg(
        F.count(F.when(F.col("plan") == "pro", 1)).alias("pro_rows"),
        F.count_if(F.col("plan") == "pro").alias("pro_events"),
        F.round(F.avg("tier_value"), 2).alias("avg_tier_value"),
        F.sum(F.when(F.col("plan") == "enterprise", F.col("tier_value"))).alias("enterprise_total_tier_value"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
        F.percentile_approx("tier_value", [0.25, 0.5, 0.75]).alias("tier_value_quartiles"),
    )
    .filter(F.col("pro_rows") >= 1)
    .orderBy(F.col("pro_rows").desc_nulls_first())
)
