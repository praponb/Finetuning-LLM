from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "right")
    .select(
        F.col("s.status"),
        F.col("s.shipped_date"),
        F.col("s.carrier"),
        F.col("o.customer_id"),
        F.col("o.amount"),
    )
    .orderBy(F.col("customer_id").desc())
)
