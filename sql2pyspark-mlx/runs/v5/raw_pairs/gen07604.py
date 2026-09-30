from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.filter(F.col("signup_date") >= "2023-08-01").alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .select(
        F.col("s.id"),
        F.col("s.status"),
        F.col("s.end_date"),
        F.col("s.customer_id"),
    )
)
