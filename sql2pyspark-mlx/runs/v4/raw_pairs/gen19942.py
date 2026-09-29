from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id", "order_id")
    .agg(F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"))
    .filter(F.col("revenue") >= 79)
    .orderBy("product_id", "order_id")
    .limit(10)
)
