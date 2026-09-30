from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([8, 9, 10]))
    .select(
        "monthly_fee",
        "end_date",
        F.datediff("end_date", "start_date").alias("days_start_date_to_end_date"),
    )
    .orderBy(F.col("end_date").desc())
)
