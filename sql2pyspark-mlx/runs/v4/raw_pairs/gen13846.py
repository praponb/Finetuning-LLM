from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.credit_limit") > 11900)
    .select(
        F.col("s.id"),
        F.col("s.monthly_fee"),
        F.col("c.age"),
        F.length(F.col("s.plan")).alias("plan_length"),
    )
)
