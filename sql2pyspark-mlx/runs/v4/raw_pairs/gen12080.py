from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.supplier_id"))
    .agg(
        F.count(F.col("oi.unit_price")).alias("unit_price_count"),
        F.countDistinct(F.col("oi.quantity")).alias("unique_orders"),
    )
    .filter(F.col("unit_price_count") >= 4)
    .orderBy("unique_orders")
)
