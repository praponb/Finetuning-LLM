from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("c.email"),
        F.col("c.age"),
        F.col("s.monthly_fee"),
        F.col("s.plan"),
        F.col("s.customer_id"),
        F.upper(F.col("s.status")).alias("status_upper"),
    )
    .orderBy("status_upper")
)
