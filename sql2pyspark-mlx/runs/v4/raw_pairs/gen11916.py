from pyspark.sql import functions as F

recent_subscriptions = (
    df_subscriptions
    .filter((F.col("status") == "CANCELLED") & (F.col("customer_id") == 26))
)

summary = (
    recent_subscriptions
    .groupBy("status", "plan")
    .agg(
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
        F.countDistinct("monthly_fee").alias("unique_customers"),
    )
)

result = (
    summary
    .filter(F.col("unique_customers") > 8)
    .select("status", "monthly_fee_range", "unique_customers")
    .orderBy(F.col("unique_customers").desc(), "status")
)
