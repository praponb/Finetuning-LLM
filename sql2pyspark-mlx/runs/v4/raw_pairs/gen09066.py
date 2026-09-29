from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").isNotNull() & F.col("product_id").isin([8, 23]))
    .groupBy("order_id", "product_id")
    .agg(
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
    .filter(F.col("revenue") < 31)
    .orderBy(F.col("avg_quantity").desc())
)
