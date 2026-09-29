from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .filter(F.month("start_date").isin([2, 10, 11]))
    .groupBy("customer_id")
    .agg(
        F.sum("monthly_fee").alias("total_monthly_fee"),
        F.max("monthly_fee").alias("max_monthly_fee"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("C"), F.col("c.id") == F.col("C.customer_id"), "inner")
    .filter(F.col("C.max_monthly_fee") > 100)
    .select(F.col("c.age"), F.col("C.total_monthly_fee"), F.col("C.max_monthly_fee"))
)
