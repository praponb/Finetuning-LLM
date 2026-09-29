from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("customer_id") == 34) & (F.year("end_date") >= 2024))
    .select(
        "id",
        "plan",
        "start_date",
        "status",
        F.last_day("start_date").alias("start_date_month_end"),
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
    )
)
