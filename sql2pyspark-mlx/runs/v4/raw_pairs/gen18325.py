from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("tier_value"),
    )
)

result = (
    flattened
    .withColumn("tier_value_tier", F.when(F.col("tier_value") >= 76, "PREMIUM").otherwise("STANDARD"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "effective_from"))
    .rollup("tier_value_tier", "quarter_start", "currency")
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.percentile_approx("tier_value", [0.25, 0.5, 0.75]).alias("tier_value_quartiles"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.percentile_approx("tier_value", [0.25, 0.5, 0.75]).alias("tier_value_quartiles_2"),
        F.sum(F.when(F.col("currency") == "THB", F.col("tier_value")).otherwise(0)).alias("thb_tier_value"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("tier_value_tier").asc_nulls_first())
)
