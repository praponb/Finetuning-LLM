from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id", "order_id")
    .agg(
        F.countDistinct("unit_price").alias("unique_orders"),
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
        F.count("unit_price").alias("unit_price_count"),
    )
    .orderBy(F.col("order_id").desc())
)
