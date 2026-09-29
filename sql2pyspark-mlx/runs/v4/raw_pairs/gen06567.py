from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.avg("quantity").alias("avg_quantity"),
        F.sum("unit_price").alias("total_unit_price"),
    )
    .orderBy("quantity_count")
)
