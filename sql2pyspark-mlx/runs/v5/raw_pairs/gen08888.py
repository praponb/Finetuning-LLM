from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.id"), F.col("c.country"))
    .agg(
        F.count(F.col("s.id")).alias("subscription_count"),
        F.count(F.col("s.monthly_fee")).alias("monthly_fee_count"),
        F.round(((100 * F.sum(F.when(F.col("s.status") == "PAUSED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_paused"),
    )
)
