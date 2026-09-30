from pyspark.sql import functions as F

customer_summary = (
    df_subscriptions
    .filter(F.year("start_date") == 2024)
    .groupBy("customer_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "ACTIVE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_active"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .filter(F.col("cs.pct_active") >= 40)
    .select(F.col("c.id"), F.col("c.segment"), F.col("cs.pct_active"))
    .orderBy("id")
)
