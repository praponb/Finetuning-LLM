from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "currency", F.explode("tier_limits").alias("tier_value"))
)

result = (
    lines
    .groupBy(
        F.when(F.col("tier_value") >= 85, "HIGH").otherwise("LOW").alias("tier_value_tier"),
    )
    .agg(
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_rows"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
    )
    .orderBy("num_currencies")
)
