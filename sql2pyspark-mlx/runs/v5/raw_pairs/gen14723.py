from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("s.status") == "EXPIRED") & (F.col("c.credit_limit") <= 11100))
    .select(
        F.col("s.end_date"),
        F.col("s.customer_id"),
        F.col("s.status"),
        F.col("c.age"),
        F.col("c.name"),
    )
)
