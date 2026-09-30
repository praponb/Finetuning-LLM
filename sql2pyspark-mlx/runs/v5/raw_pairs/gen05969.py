from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.filter(F.col("store_id").isin([6, 16, 23, 34])).alias("o"), F.col("oi.order_id") == F.col("o.id"), "left_semi")
    .select(F.col("oi.id"), F.col("oi.unit_price"))
)
