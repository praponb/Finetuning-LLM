from pyspark.sql import functions as F

customer_summary = (
    df_subscriptions
    .filter(F.col("start_date") < "2024-03-20")
    .groupBy("customer_id")
    .agg(F.count("monthly_fee").alias("monthly_fee_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.monthly_fee_count") > 9)
    .select(F.col("c.id"), F.col("c.city"), F.col("t.monthly_fee_count"))
    .orderBy("id")
    .limit(5)
)
