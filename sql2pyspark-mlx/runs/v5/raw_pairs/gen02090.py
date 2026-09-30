from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "outer")
    .groupBy(F.col("o.store_id"))
    .agg(F.avg(F.col("oi.quantity")).alias("avg_quantity"))
    .filter(F.col("avg_quantity") > 3.9)
    .orderBy(F.col("store_id").desc(), F.col("avg_quantity").desc())
)
