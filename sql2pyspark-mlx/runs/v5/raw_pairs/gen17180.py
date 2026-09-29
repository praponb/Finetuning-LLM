from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("discounts").alias("tier_value"))
)

result = (
    exploded
    .groupBy(F.date_trunc("QUARTER", "effective_from").alias("quarter_start"))
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.when(F.col("plan") == "pro", F.col("tier_value")).otherwise(0)).alias("pro_tier_value"),
        F.percentile_approx("tier_value", [0.25, 0.5, 0.75]).alias("tier_value_quartiles"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
        F.count_if(F.col("plan") == "enterprise").alias("enterprise_events"),
        F.countDistinct("plan").alias("distinct_plans"),
    )
    .orderBy(F.col("enterprise_events").asc_nulls_first())
)
