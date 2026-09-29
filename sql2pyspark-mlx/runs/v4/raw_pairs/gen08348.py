from pyspark.sql import functions as F

recent_subscriptions = (
    df_subscriptions
    .filter(F.col("status").isin(["CANCELLED", "PAUSED", "ACTIVE"]))
    .withColumn("status_upper", F.upper("status"))
)

plan_summary = (
    recent_subscriptions
    .groupBy(
        "plan",
        F.when(F.col("monthly_fee") >= 199, "high").when(F.col("monthly_fee") >= 101, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(F.count("*").alias("row_count"))
)

result = plan_summary.orderBy(F.col("row_count").desc())
