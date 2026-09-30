from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("c.credit_limit").between(10800, 12100) & ~F.col("c.is_active"))
    .select(
        F.col("s.id"),
        F.col("s.plan"),
        F.col("s.customer_id"),
        F.col("s.status"),
        F.col("c.name"),
        F.col("c.is_active"),
    )
)
