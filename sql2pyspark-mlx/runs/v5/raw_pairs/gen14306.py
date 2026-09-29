from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date").between("2023-12-01", "2025-01-01") & F.col("status").isin(["ACTIVE", "PAUSED", "CANCELLED"]))
    .select(
        "id",
        "plan",
        "status",
        F.last_day("start_date").alias("start_date_month_end"),
        F.date_trunc("WEEK", "end_date").alias("end_date_period"),
    )
)
