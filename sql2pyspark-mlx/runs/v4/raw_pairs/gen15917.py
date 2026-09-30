from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([3, 6, 23, 24]))
    .select(
        "plan",
        "start_date",
        F.last_day("end_date").alias("end_date_month_end"),
        F.round("monthly_fee", 2).alias("monthly_fee_rounded"),
    )
    .orderBy("start_date")
)
