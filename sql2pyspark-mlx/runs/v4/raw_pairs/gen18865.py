from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(160, 240))
    .groupBy("customer_id")
    .agg(F.count("monthly_fee").alias("monthly_fee_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.country"),
        F.col("c.age"),
        F.col("cs.monthly_fee_count"),
    )
    .orderBy(F.col("age").desc())
)
