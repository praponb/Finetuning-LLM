from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.filter(F.col("status").isin(["ACTIVE", "PAUSED", "CANCELLED"])).alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_semi")
    .select(
        F.col("c.id"),
        F.col("c.is_active"),
        F.col("c.signup_date"),
        F.col("c.city"),
    )
    .orderBy(F.col("id").desc())
)
