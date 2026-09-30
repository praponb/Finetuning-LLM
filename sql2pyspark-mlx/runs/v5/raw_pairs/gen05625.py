from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .filter(F.col("c.name").like("%Grace%") & F.col("c.credit_limit").between(12000, 13100))
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("o.customer_id"),
        F.col("c.is_active"),
        F.col("c.email"),
        F.col("c.signup_date"),
    )
    .orderBy("signup_date")
)
