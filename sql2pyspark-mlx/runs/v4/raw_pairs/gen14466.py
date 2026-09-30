from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "outer")
    .select(
        F.col("s.id"),
        F.col("s.monthly_fee"),
        F.col("s.start_date"),
        F.col("s.status"),
        F.col("c.signup_date"),
        F.col("c.name"),
        F.ceil(F.col("s.monthly_fee")).alias("monthly_fee_whole"),
    )
)
