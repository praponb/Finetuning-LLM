from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "left")
    .groupBy(F.col("p.category"), F.col("p.brand"))
    .agg(
        F.count(F.col("oi.unit_price")).alias("unit_price_count"),
        F.min(F.col("oi.quantity")).alias("min_quantity"),
    )
    .filter(F.col("unit_price_count") >= 4)
    .orderBy("unit_price_count", "category", "brand")
    .limit(50)
)
