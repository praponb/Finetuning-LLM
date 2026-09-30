from pyspark.sql import functions as F

customer_summary = (
    df_subscriptions
    .groupBy("customer_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cancelled"),
        F.round(F.stddev("monthly_fee"), 2).alias("stddev_monthly_fee"),
    )
)

result = (
    df_customers.alias("customers")
    .join(customer_summary.alias("customer_summary"), F.col("customers.id") == F.col("customer_summary.customer_id"), "left")
    .filter(F.col("customer_summary.pct_cancelled") > 52)
    .select(
        F.col("customers.id"),
        F.col("customers.signup_date"),
        F.col("customer_summary.pct_cancelled"),
        F.col("customer_summary.stddev_monthly_fee"),
    )
)
