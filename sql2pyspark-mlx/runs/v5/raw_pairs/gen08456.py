from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "outer")
    .select(
        F.col("s.monthly_fee"),
        F.col("s.customer_id"),
        F.col("c.age"),
        F.col("c.segment"),
        F.col("c.email"),
    )
)
