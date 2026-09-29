from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "right")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.delivered_date"),
        F.col("s.carrier"),
        F.col("o.store_id"),
        F.col("o.order_date"),
        F.col("c.country"),
        F.col("c.credit_limit"),
    )
)
