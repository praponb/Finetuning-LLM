from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(F.count(F.when(F.col("status") == "EXPIRED", 1)).alias("expired_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .filter(F.col("cs.expired_count") > 4)
    .select(
        F.col("c.id"),
        F.col("c.segment"),
        F.col("c.email"),
        F.col("cs.expired_count"),
    )
)
