from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .select(
        F.col("s.status"),
        F.col("s.order_id"),
        F.col("o.status").alias("order_status"),
        F.col("o.store_id"),
        F.col("c.email"),
    )
)
