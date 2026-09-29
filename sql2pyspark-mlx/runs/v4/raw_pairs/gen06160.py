from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("c.credit_limit") <= 12300) & (F.length(F.col("c.name")) < 11))
    .select(
        F.col("s.id"),
        F.col("s.delivered_date"),
        F.col("o.order_date"),
        F.col("c.country"),
        F.col("c.signup_date"),
    )
)
