from pyspark.sql import functions as F

valid_subscriptions = (
    df_subscriptions
    .filter((F.col("monthly_fee") <= 230) & (F.col("customer_id") == 4))
    .withColumn("days_since_start_date", F.datediff(F.current_date(), "start_date"))
)

status_summary = (
    valid_subscriptions
    .groupBy("status", "plan")
    .agg(
        F.round(F.stddev("days_since_start_date"), 2).alias("stddev_days_since_start_date"),
        F.round(F.avg("days_since_start_date"), 2).alias("avg_days_since_start_date"),
    )
)

result = (
    status_summary
    .filter(F.col("stddev_days_since_start_date") < 48)
    .orderBy(F.col("avg_days_since_start_date").desc())
)
