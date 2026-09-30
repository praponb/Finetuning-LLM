from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_limits",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    parsed
    .groupBy(F.date_trunc("QUARTER", "effective_from").alias("quarter_start"))
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.count_if(F.col("plan") == "enterprise").alias("enterprise_events"),
        F.size(F.collect_set("plan")).alias("num_plans"),
    )
    .filter(F.col("enterprise_events") >= 1)
    .orderBy(F.col("num_plans").desc())
)
