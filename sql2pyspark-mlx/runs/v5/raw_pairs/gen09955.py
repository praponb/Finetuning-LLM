from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNull())
    .groupBy(F.date_format("start_date", "yyyy-MM").alias("start_month"))
    .agg(
        F.count("*").alias("num_subscriptions"),
        F.max("monthly_fee").alias("max_monthly_fee"),
    )
    .filter(F.col("max_monthly_fee") > 70)
    .select("start_month", "num_subscriptions", "max_monthly_fee")
)
