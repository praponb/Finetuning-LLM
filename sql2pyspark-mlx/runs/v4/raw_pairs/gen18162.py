from pyspark.sql import functions as F

customer_totals = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "left")
    .filter(F.col("ct.monthly_fee_count") > 6)
    .select(
        F.col("c.id"),
        F.col("c.is_active"),
        F.col("ct.monthly_fee_count"),
        F.col("ct.monthly_fee_range"),
    )
    .orderBy(F.col("monthly_fee_count").desc())
)
