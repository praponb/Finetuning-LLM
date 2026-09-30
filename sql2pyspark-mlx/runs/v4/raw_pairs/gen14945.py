from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.credit_limit").between(9000, 14700))
    .select(
        F.col("s.id"),
        F.col("s.cost"),
        F.col("o.status"),
        F.col("o.customer_id"),
        F.col("c.segment"),
        F.col("c.signup_date"),
    )
    .orderBy("status", "cost")
)
