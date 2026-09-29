from pyspark.sql import functions as F

filtered = (
    df_inventory
    .filter(F.col("product_id").isin([2, 5, 8]))
    .withColumn("quantity_filled", F.coalesce("quantity", F.lit(0)))
)

result = (
    filtered
    .groupBy("product_id", F.date_trunc("WEEK", "last_restock").alias("week_start"))
    .agg(
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.round(F.avg("quantity_filled"), 2).alias("avg_quantity_filled"),
    )
    .orderBy(F.col("product_id").desc_nulls_last())
)
