from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "PAUSED")
    .select(
        "id",
        "end_date",
        "status",
        "monthly_fee",
        F.last_day("end_date").alias("end_date_month_end"),
    )
    .orderBy("end_date", "id")
    .limit(3)
)
