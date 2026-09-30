from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .filter(F.col("o.status").isin(["PENDING", "COMPLETED"]) | (F.col("o.store_id") == 27))
    .select(F.col("o.shipping_fee"), F.col("o.discount"), F.col("oi.unit_price"))
)
