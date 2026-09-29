from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_limits",
        "tier_prices",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    flattened_attrs
    .withColumn("quarter_start", F.date_trunc("QUARTER", "effective_from"))
    .groupingSets(
        [["quarter_start", "plan"], ["quarter_start"], []],
        "quarter_start",
        "plan",
    )
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("plan") == "starter", 1)).alias("starter_count"),
    )
    .orderBy(F.col("quarter_start").desc())
)
