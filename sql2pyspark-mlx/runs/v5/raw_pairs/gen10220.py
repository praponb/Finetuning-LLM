from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "left")
    .select(
        F.col("o.store_id"),
        F.col("o.channel"),
        F.col("o.amount"),
        F.col("s.carrier"),
        F.col("s.shipped_date"),
        F.col("s.order_id"),
    )
)
