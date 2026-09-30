from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.segment"))
    .agg(
        F.max(F.col("s.monthly_fee")).alias("max_monthly_fee"),
        F.round(F.avg(F.col("s.monthly_fee")), 2).alias("avg_monthly_fee"),
    )
    .filter(F.col("avg_monthly_fee") > 29)
    .orderBy("avg_monthly_fee")
)
