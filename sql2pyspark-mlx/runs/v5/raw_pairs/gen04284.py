from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("product_id") == 14) & F.col("order_id").isin([9, 25, 39]))
    .groupBy("order_id", "product_id")
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
    )
    .filter(F.col("revenue") < 52)
    .orderBy("product_id")
)
