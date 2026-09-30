from pyspark.sql import functions as F

filtered = (
    df_order_items
    .filter(F.col("order_id") == 30)
    .select("*", F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"))
)

result = (
    filtered
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 7, "HIGH").otherwise("LOW"))
    .cube("quantity_tier")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("unit_price", 0.95).alias("unit_price_p95"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("distinct_ids").desc())
)
