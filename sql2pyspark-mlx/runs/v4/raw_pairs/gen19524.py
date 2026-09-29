from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .filter(F.col("plan") != "basic")
    .select(
        "id",
        "plan",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    parsed
    .rollup("plan")
    .agg(
        F.count("*").alias("num_rows"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.sort_array(F.collect_set("plan")).alias("plan_list_2"),
        F.count(F.when(F.col("plan") == "pro", 1)).alias("pro_rows"),
        F.grouping("plan").alias("gid"),
    )
)
