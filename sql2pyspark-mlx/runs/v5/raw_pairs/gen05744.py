from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.city"), F.col("c.segment"))
    .agg(
        F.count(F.when(F.col("s.status") == "EXPIRED", 1)).alias("expired_count"),
        F.count(F.col("s.monthly_fee")).alias("monthly_fee_count"),
    )
)
