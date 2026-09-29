from pyspark.sql import functions as F

flattened = df_price_tiers.select("id", "plan", F.explode("tier_limits").alias("tier_value"))

result = (
    flattened
    .groupBy(
        F.when(F.col("tier_value") >= 87, "HIGH").otherwise("LOW").alias("tier_value_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sum("tier_value").alias("total_tier_value"),
        F.count(F.when(F.col("plan") == "starter", 1)).alias("starter_count"),
    )
    .orderBy(F.col("tier_value_tier").asc_nulls_last())
)
