from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left_semi")
    .filter(F.month(F.col("o.order_date")).isin([4, 7, 8]))
    .select(F.col("o.id"), F.col("o.customer_id"), F.col("o.store_id"))
    .orderBy(F.col("id").desc(), F.col("customer_id").desc())
)
