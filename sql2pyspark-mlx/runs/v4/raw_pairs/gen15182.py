from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2024-04-14").cast("date"))
    .withColumn("quantity_band", F.when(F.col("quantity") >= 214, "High").when(F.col("quantity") >= 106, "Medium").otherwise("Low"))
)

result = (
    base
    .groupBy("store_id", "product_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.avg("quantity").alias("avg_quantity"),
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
        F.min("quantity").alias("min_quantity"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
    )
)
