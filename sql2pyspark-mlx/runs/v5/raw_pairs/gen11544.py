from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.brand"))
    .agg(
        F.count(F.col("oi.id")).alias("order_item_count"),
        F.min(F.col("oi.quantity")).alias("min_quantity"),
        F.countDistinct(F.col("oi.unit_price")).alias("unique_products"),
    )
)
