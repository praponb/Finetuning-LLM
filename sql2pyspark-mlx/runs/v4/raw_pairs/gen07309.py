from pyspark.sql import functions as F

valid = (
    df_subscriptions
    .filter((F.col("end_date") >= F.lit("2023-09-11").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("monthly_fee_filled", F.coalesce("monthly_fee", F.lit(0)))
    .withColumn("start_date_month_end", F.last_day("start_date"))
)

result = (
    valid
    .groupBy(
        F.when(F.col("monthly_fee") >= 250, "LARGE").otherwise("SMALL").alias("monthly_fee_tier"),
    )
    .agg(
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"),
        F.count_if(F.col("status") == "ACTIVE").alias("active_events"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.max(F.when(F.col("plan") == "premium", F.col("monthly_fee"))).alias("premium_max_monthly_fee"),
        F.round(F.sum("monthly_fee_filled") / F.nullif(F.count(F.when(F.col("plan") == "enterprise", 1)), F.lit(0)), 2).alias("monthly_fee_filled_per_enterprise"),
    )
    .filter(F.col("active_events") > 1)
)
