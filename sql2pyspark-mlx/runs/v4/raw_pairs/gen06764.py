from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.filter(F.col("product_id").isin([1, 2, 20, 26])).alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left_semi")
    .select(F.col("o.id"), F.col("o.customer_id"))
    .orderBy(F.col("id").desc())
)
