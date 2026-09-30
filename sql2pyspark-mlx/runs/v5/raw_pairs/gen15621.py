from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

result = (
    exploded
    .groupBy("currency")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("event_count"),
        F.count("*").alias("num_rows"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.sum(F.when(F.col("currency") == "USD", F.col("tier_value")).otherwise(0)).alias("usd_tier_value"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_rows"),
    )
)
