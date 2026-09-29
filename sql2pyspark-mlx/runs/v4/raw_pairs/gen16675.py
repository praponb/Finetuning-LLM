from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("plan") == "premium") | F.col("end_date").between("2024-07-07", "2025-05-01"))
    .select(
        "start_date",
        F.date_format("end_date", "MMM yyyy").alias("end_date_label"),
        F.date_trunc("MONTH", "end_date").alias("end_date_period"),
    )
)
