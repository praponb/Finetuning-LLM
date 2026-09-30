from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .filter(F.col("p.launch_date").between("2023-09-10", "2024-02-04"))
    .groupBy(F.month(F.col("o.order_date")).alias("order_month"))
    .agg(F.count(F.col("oi.unit_price")).alias("unit_price_count"))
    .orderBy(F.col("order_month").desc())
)
