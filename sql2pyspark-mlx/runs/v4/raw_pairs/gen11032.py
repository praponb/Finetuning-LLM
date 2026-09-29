from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.status"),
        F.col("o.store_id"),
        F.col("o.discount"),
        F.col("c.segment"),
        F.col("c.credit_limit"),
    )
)
