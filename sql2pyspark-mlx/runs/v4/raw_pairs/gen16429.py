from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.category"))
    .agg(F.avg(F.col("oi.quantity")).alias("avg_quantity"))
    .orderBy(F.col("avg_quantity").desc())
)
