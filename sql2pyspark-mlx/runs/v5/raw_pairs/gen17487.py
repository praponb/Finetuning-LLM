from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.price") < 400)
    .groupBy(F.col("o.status"))
    .agg(F.max(F.col("oi.quantity")).alias("max_quantity"))
    .orderBy("status")
)
