from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.id"), F.col("c.email"))
    .agg(
        F.count(F.col("s.id")).alias("subscription_count"),
        F.round(F.avg(F.col("s.monthly_fee")), 2).alias("avg_monthly_fee"),
    )
    .filter(F.col("avg_monthly_fee") > 56)
    .orderBy("email")
)
