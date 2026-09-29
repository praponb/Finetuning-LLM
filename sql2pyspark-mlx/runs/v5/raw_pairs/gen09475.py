from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "PAUSED")
    .select(
        "customer_id",
        "end_date",
        "monthly_fee",
        F.last_day("end_date").alias("end_date_month_end"),
    )
)
