from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .select(
        F.col("s.carrier"),
        F.col("s.order_id"),
        F.col("o.amount"),
        F.col("o.status"),
        F.col("c.name"),
        F.col("c.signup_date"),
    )
    .orderBy(F.col("signup_date").desc())
)
