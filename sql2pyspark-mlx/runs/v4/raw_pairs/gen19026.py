from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(F.col("last_restock") >= "2024-04-14")
    .withColumn("quantity_rounded", F.round("quantity", 1))
    .withColumn("quantity_level", F.when(F.col("quantity") >= 193, "High").when(F.col("quantity") >= 48, "Medium").otherwise("Low"))
)

result = (
    base
    .rollup(F.date_trunc("month", "last_restock").alias("activity_month"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("event_count"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
    .filter(F.col("distinct_ids") > 1)
)
