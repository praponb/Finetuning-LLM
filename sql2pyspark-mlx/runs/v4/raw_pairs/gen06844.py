from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "right")
    .select(
        F.col("s.id"),
        F.col("s.cost"),
        F.col("s.order_id"),
        F.col("s.delivered_date"),
        F.col("o.discount"),
        F.col("o.customer_id"),
    )
    .orderBy(F.col("customer_id").desc())
)
