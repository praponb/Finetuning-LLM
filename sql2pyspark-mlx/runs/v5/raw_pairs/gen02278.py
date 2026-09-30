from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.countDistinct("quantity").alias("unique_orders"),
        F.max("quantity").alias("max_quantity"),
    )
    .filter(F.col("unique_orders") > 5)
)
