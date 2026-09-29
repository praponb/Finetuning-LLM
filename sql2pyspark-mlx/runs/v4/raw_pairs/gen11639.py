from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("s.status") == "IN_TRANSIT")
    .select(
        F.col("s.delivered_date"),
        F.col("s.shipped_date"),
        F.col("o.discount"),
        F.col("o.customer_id"),
        F.col("o.status"),
    )
)
