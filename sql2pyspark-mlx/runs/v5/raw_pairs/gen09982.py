from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "outer")
    .filter(F.col("o.amount").between(640, 870))
    .groupBy(F.col("p.brand"))
    .agg(F.max(F.col("oi.unit_price")).alias("max_unit_price"))
)
