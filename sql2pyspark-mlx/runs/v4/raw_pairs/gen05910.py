from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(((F.col("customer_id") == 29) & (F.col("status") == "EXPIRED")) | (F.col("plan") != "basic"))
)

summary = (
    base
    .groupBy(F.date_format("start_date", "yyyy-MM").alias("start_month"))
    .agg(
        F.min("start_date").alias("min_start_date"),
        F.count("monthly_fee").alias("monthly_fee_count"),
    )
)

result = (
    summary
    .select("start_month", "min_start_date", "monthly_fee_count")
    .orderBy("start_month")
)
