from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("c.city"),
        F.col("c.name"),
        F.col("c.signup_date"),
    )
)
