from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.name").like("%Par%") & (F.col("c.signup_date") <= "2023-04-15"))
    .select(
        F.col("p.order_id"),
        F.col("p.amount"),
        F.col("o.channel"),
        F.col("c.country"),
        F.lower(F.col("p.status")).alias("status_lower"),
    )
)
