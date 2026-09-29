from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 5, "mid").otherwise("low").alias("quantity_bucket"),
        "order_id",
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.countDistinct("quantity").alias("unique_orders"),
        F.max("quantity").alias("max_quantity"),
    )
    .filter(F.col("avg_quantity") >= 78)
)
