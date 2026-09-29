from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id", "product_id")
    .agg(
        F.count("*").alias("total_count"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .filter(F.col("avg_quantity") > 3.7)
)
