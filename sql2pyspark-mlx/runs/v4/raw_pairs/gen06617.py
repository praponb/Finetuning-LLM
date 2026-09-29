from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
    .filter(F.col("quantity_count") > 6)
    .select("quantity_bucket", "avg_quantity", "quantity_count")
)
