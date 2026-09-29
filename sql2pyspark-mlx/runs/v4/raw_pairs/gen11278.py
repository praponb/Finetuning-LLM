from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .groupBy(F.col("c.segment"))
    .agg(
        F.round(F.avg(F.col("s.monthly_fee")), 2).alias("avg_monthly_fee"),
        F.max(F.col("s.end_date")).alias("latest_end_date"),
    )
    .orderBy("latest_end_date")
)
