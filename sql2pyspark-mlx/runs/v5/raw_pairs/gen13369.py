from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(~F.col("s.status").isin(["RETURNED", "DELIVERED", "LOST"]))
    .select(
        F.col("s.id"),
        F.col("s.order_id"),
        F.col("s.status"),
        F.col("s.delivered_date"),
        F.col("o.discount"),
    )
)
