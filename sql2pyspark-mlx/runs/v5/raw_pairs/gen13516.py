from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("s.end_date"),
        F.col("s.monthly_fee"),
        F.col("c.credit_limit"),
        F.col("c.age"),
        F.col("c.is_active"),
        F.when(F.col("s.status") == "EXPIRED", 1).otherwise(0).alias("is_expired"),
    )
    .orderBy("end_date", F.col("customer_id").desc())
)
