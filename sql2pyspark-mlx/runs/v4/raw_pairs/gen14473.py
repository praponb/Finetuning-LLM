from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("s.status").isin(["IN_TRANSIT", "LOST"]) & (F.col("o.customer_id") == 11))
    .select(
        F.col("s.id"),
        F.col("s.shipped_date"),
        F.col("o.shipping_fee"),
        F.ceil(F.col("s.cost")).alias("cost_whole"),
    )
)
