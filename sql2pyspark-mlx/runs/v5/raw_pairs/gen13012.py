from pyspark.sql import functions as F

enriched = (
    df_inventory
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .filter(F.array_contains(F.col("b.tags"), "bestseller"))
    .select("src.*", F.col("b.product_name"))
)

result = (
    enriched
    .withColumn("quarter_start", F.date_trunc("QUARTER", "last_restock"))
    .withColumn("activity_month", F.date_trunc("MONTH", "last_restock"))
    .rollup("quarter_start", "activity_month")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
        F.grouping_id().alias("grouping_level"),
    )
)
