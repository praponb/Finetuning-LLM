from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.delivered_date"),
        F.col("o.amount"),
        F.col("o.customer_id"),
        F.col("c.country"),
    )
)
