from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.amount") <= 700)
    .select(
        F.col("o.customer_id"),
        F.col("o.status"),
        F.col("o.channel"),
        F.col("s.shipped_date"),
        F.col("s.carrier"),
    )
)
