from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.plan"),
        F.col("s.status"),
        F.col("c.signup_date"),
        F.col("c.city"),
        F.col("c.segment"),
    )
    .orderBy("id")
    .limit(10)
)
