from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .filter(F.col("oi.order_id").isin([8, 16, 29, 34]))
    .groupBy(F.col("p.id"), F.col("p.product_name"))
    .agg(
        F.count(F.col("oi.id")).alias("order_item_count"),
        F.round(F.stddev(F.col("oi.unit_price")), 2).alias("stddev_unit_price"),
    )
)
