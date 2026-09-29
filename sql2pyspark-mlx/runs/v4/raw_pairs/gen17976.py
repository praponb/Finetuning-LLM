from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id") == 30)
    .groupBy("order_id")
    .agg(
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
        F.count("unit_price").alias("unit_price_count"),
    )
    .filter(F.col("unit_price_count") >= 3)
    .orderBy("order_id")
)
