from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.plan"),
        F.col("s.end_date"),
        F.col("c.city"),
        F.col("c.country"),
        F.when(F.col("s.status") == "EXPIRED", 1).otherwise(0).alias("is_expired"),
    )
)
