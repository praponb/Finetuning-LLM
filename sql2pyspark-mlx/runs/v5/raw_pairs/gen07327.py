from pyspark.sql import functions as F

customer_totals = (
    df_subscriptions
    .filter(F.col("customer_id").isNull())
    .groupBy("customer_id")
    .agg(
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.sum(F.when(F.col("plan") == "enterprise", 1).otherwise(0)).alias("enterprise_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "inner")
    .filter(F.col("ct.avg_monthly_fee") >= 150)
    .select(
        F.col("c.id"),
        F.col("c.signup_date"),
        F.col("ct.avg_monthly_fee"),
        F.col("ct.enterprise_count"),
    )
    .orderBy(F.col("avg_monthly_fee").desc(), "signup_date")
)
