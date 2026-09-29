from pyspark.sql import functions as F

customer_summary = (
    df_subscriptions
    .filter(F.col("customer_id") == 40)
    .groupBy("customer_id")
    .agg(
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
        F.count(F.when(F.col("status") == "EXPIRED", 1)).alias("expired_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .filter(F.col("cs.expired_count") >= 6)
    .select(
        F.col("c.email"),
        F.col("c.age"),
        F.col("cs.monthly_fee_range"),
        F.col("cs.expired_count"),
    )
)
