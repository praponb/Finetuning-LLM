from pyspark.sql import functions as F

recent_subscriptions = (
    df_subscriptions
    .filter(F.col("customer_id").isNull() & (F.year("start_date") == 2023))
    .withColumn("monthly_fee_rounded", F.round("monthly_fee", 1))
)

start_quarter_summary = (
    recent_subscriptions
    .groupBy(F.quarter("start_date").alias("start_quarter"), "status")
    .agg(F.count("monthly_fee_rounded").alias("monthly_fee_rounded_count"))
)

result = (
    start_quarter_summary
    .select("start_quarter", "status", "monthly_fee_rounded_count")
    .orderBy("start_quarter")
)
