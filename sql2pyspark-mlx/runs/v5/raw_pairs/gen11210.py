from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([13, 18, 23, 29]))
    .groupBy("product_id", "order_id")
    .agg(F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"))
)
