from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([12, 19, 34]))
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
    )
)
