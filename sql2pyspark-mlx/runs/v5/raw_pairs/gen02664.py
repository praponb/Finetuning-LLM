from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .groupBy(F.col("c.id"), F.col("c.country"))
    .agg(
        F.count(F.col("s.id")).alias("subscription_count"),
        F.max(F.col("s.start_date")).alias("last_start_date"),
        F.count(F.when(F.col("s.plan") == "enterprise", 1)).alias("enterprise_count"),
    )
    .orderBy(F.col("enterprise_count").desc(), "subscription_count")
)
