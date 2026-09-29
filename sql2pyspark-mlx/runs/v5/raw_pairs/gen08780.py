from pyspark.sql import functions as F

filtered = (
    df_subscriptions
    .filter((F.col("end_date") >= F.to_date(F.lit("2023-10-20"))) & (F.col("end_date") >= "2024-01-01"))
    .select(
        "*",
        F.date_trunc("month", "start_date").alias("start_date_period"),
        F.round((F.col("monthly_fee") * 0.9), 2).alias("monthly_fee_scaled"),
    )
)

result = (
    filtered
    .groupBy("customer_id")
    .pivot("status", ["CANCELLED", "EXPIRED"])
    .agg(F.avg("monthly_fee").alias("value_sum"), F.count(F.lit(1)).alias("item_count"))
)
