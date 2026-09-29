from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "outer")
    .select(F.col("oi.unit_price"), F.col("o.customer_id"), F.col("p.product_name"))
    .orderBy(F.col("product_name").desc())
)
