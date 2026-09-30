from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.id"), F.col("p.brand"))
    .agg(
        F.count(F.col("oi.id")).alias("order_item_count"),
        F.round(F.avg(F.col("oi.unit_price")), 2).alias("avg_unit_price"),
        F.min(F.col("oi.unit_price")).alias("min_unit_price"),
    )
    .orderBy(F.col("avg_unit_price").desc())
)
