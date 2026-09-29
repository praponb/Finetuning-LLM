from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("tier_value"),
    )
)

result = (
    lines
    .rollup(
        F.when(F.col("tier_value") >= 75, "HIGH").otherwise("LOW").alias("tier_value_tier"),
        "plan",
    )
    .agg(
        F.percentile_approx("tier_value", 0.95).alias("tier_value_p95"),
        F.count_if(F.col("plan") == "starter").alias("starter_events"),
    )
    .orderBy(F.col("tier_value_p95").desc())
)
