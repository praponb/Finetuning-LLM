from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "right")
    .filter(F.col("p.supplier_id").isin([15, 33, 38]))
    .groupBy(F.col("p.brand"))
    .agg(
        F.sum((F.col("oi.quantity") * F.col("oi.unit_price"))).alias("revenue"),
        F.round(F.stddev(F.col("oi.quantity")), 2).alias("stddev_quantity"),
    )
    .orderBy("revenue", "brand")
    .limit(1)
)
