from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("c.signup_date"),
        F.col("c.segment"),
        F.length(F.col("s.plan")).alias("plan_length"),
    )
)
