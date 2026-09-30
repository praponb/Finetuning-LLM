from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select(
        F.col("s.delivered_date"),
        F.col("s.status"),
        F.col("s.order_id"),
        F.col("o.shipping_fee"),
    )
    .orderBy("shipping_fee")
)
