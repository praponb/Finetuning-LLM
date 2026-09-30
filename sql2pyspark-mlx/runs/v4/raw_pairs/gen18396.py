from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "right")
    .groupBy(F.col("c.is_active"))
    .agg(
        F.max(F.col("s.end_date")).alias("last_end_date"),
        F.sum(F.when(F.col("s.status") == "PAUSED", F.col("s.monthly_fee")).otherwise(0)).alias("paused_monthly_fee"),
    )
    .orderBy(F.col("last_end_date").desc())
)
