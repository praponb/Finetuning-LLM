from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("p.category"))
    .agg(F.round(F.avg(F.col("oi.quantity")), 2).alias("avg_quantity"))
    .filter(F.col("avg_quantity") > 28)
)
