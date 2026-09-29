from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(3, 5) & (F.col("unit_price") < 140))
    .groupBy("order_id")
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
    )
)
