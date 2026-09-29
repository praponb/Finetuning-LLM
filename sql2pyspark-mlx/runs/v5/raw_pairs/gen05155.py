from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("order_id") == 25) & F.col("product_id").isin([1, 12, 24, 35]) & F.col("quantity").between(4, 8))
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
    )
)
