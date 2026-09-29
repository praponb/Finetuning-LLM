from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .filter(F.col("oi.order_id").isin([5, 9, 38]))
    .groupBy(
        F.when(F.col("p.price") >= 1050, "high").when(F.col("p.price") >= 420, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(
        F.sum((F.col("oi.unit_price") * F.col("oi.quantity"))).alias("revenue"),
        F.round(F.stddev(F.col("oi.unit_price")), 2).alias("stddev_unit_price"),
    )
)
