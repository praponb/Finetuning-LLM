from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.countDistinct("unit_price").alias("unique_orders"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .filter(F.col("unique_orders") >= 10)
    .select("product_id", "unique_orders", "avg_quantity")
    .orderBy(F.col("product_id").desc())
)
