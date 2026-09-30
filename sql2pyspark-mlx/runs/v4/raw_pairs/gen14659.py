from pyspark.sql import functions as F

filtered = (
    df_subscriptions
    .filter((F.col("end_date") > F.lit("2024-03-07").cast("date")) & (F.col("end_date") <= "2025-01-01"))
    .withColumn("clean_status", F.trim("status"))
)

result = (
    filtered
    .groupBy("status", "customer_id")
    .agg(
        F.stddev_samp("monthly_fee").alias("monthly_fee_stddev"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.sum(F.when(F.col("status") == "ACTIVE", F.col("monthly_fee")).otherwise(0)).alias("active_monthly_fee"),
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("status") == "ACTIVE", 1)), F.lit(0)), 2).alias("monthly_fee_per_active"),
        F.sum(F.when(F.col("status") == "EXPIRED", F.col("monthly_fee")).otherwise(0)).alias("expired_monthly_fee"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
    .orderBy(F.col("active_monthly_fee").desc())
)
