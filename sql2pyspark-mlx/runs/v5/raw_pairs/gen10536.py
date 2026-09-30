from pyspark.sql import functions as F

subscriptions_by_customer = (
    df_subscriptions
    .filter(F.col("status") == "PAUSED")
    .groupBy("customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.count("monthly_fee").alias("monthly_fee_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(subscriptions_by_customer.alias("sbc"), F.col("c.id") == F.col("sbc.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.country"),
        F.col("sbc.status_values"),
        F.col("sbc.monthly_fee_count"),
    )
    .orderBy("country")
)
