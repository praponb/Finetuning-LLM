from pyspark.sql import functions as F

customer_summary = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
)

result = (
    df_customers.alias("customers")
    .join(customer_summary.alias("t"), F.col("customers.id") == F.col("t.customer_id"), "inner")
    .select(F.col("customers.email"), F.col("t.total_monthly_fee"))
    .orderBy("total_monthly_fee")
)
