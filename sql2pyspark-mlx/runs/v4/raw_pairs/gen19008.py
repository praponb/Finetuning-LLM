from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .select(F.col("c.city"), F.col("t.monthly_fee_range"), F.col("t.avg_monthly_fee"))
    .orderBy(F.col("city").desc())
)
