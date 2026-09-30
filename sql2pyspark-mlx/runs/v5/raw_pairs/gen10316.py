from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("val"),
    )
)

result = (
    flattened
    .groupBy("currency", "plan")
    .agg(
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
        F.countDistinct("currency").alias("distinct_currencies"),
        F.round(F.avg("val"), 2).alias("avg_val"),
        F.sum(F.when(F.col("currency") == "USD", F.col("val"))).alias("usd_total_val"),
    )
)
