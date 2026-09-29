from pyspark.sql import functions as F

flattened = df_price_tiers.select("id", "plan", F.explode("tier_prices").alias("tier_value"))

result = (
    flattened
    .cube("plan")
    .agg(
        F.count_if(F.col("plan") == "pro").alias("pro_events"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.count_if(F.col("plan") == "pro").alias("pro_events_2"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("plan").asc_nulls_last())
)
