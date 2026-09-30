from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_limits").alias("element"))
)

result = (
    flattened
    .cube("currency")
    .agg(
        F.percentile_approx("element", 0.95).alias("element_p95"),
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_rows"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("element_p95") >= 0)
)
