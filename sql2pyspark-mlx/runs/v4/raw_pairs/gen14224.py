from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select(
        F.col("s.shipped_date"),
        F.col("s.cost"),
        F.col("s.carrier"),
        F.col("o.order_date"),
        F.col("o.status"),
        F.col("o.customer_id"),
    )
)
