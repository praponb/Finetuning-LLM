from pyspark.sql import functions as F

subscriptions_by_customer = (
    df_subscriptions
    .filter(F.col("start_date") <= "2025-08-01")
    .groupBy("customer_id")
    .agg(F.min("end_date").alias("min_end_date"))
)

result = (
    df_customers.alias("c")
    .join(subscriptions_by_customer.alias("sbc"), F.col("c.id") == F.col("sbc.customer_id"), "inner")
    .select(F.col("c.city"), F.col("sbc.min_end_date"))
    .orderBy(F.col("min_end_date").desc())
)
