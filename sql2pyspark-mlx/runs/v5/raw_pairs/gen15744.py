from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("status") == "EXPIRED", 1)).alias("expired_count"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.enterprise_count") >= 7)
    .select(
        F.col("c.id"),
        F.col("c.signup_date"),
        F.col("t.expired_count"),
        F.col("t.enterprise_count"),
    )
)
