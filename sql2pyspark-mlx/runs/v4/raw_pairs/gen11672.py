from pyspark.sql import functions as F

recent_subscriptions = (
    df_subscriptions
    .filter(F.month("start_date").isin([1, 6, 12]) & (F.col("customer_id") == 24) & (F.col("monthly_fee") >= 190))
    .withColumn("plan_upper", F.upper("plan"))
)

status_summary = (
    recent_subscriptions
    .groupBy(
        "status",
        F.when(F.col("monthly_fee") >= 204, "high").when(F.col("monthly_fee") >= 76, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(F.min("monthly_fee").alias("min_monthly_fee"))
)

result = (
    status_summary
    .select("status", "monthly_fee_bucket", "min_monthly_fee")
    .orderBy("min_monthly_fee")
)
