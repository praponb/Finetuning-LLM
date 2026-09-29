from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "ACTIVE")
    .select(
        "id",
        "status",
        "monthly_fee",
        F.date_format("start_date", "yyyy-MM-dd").alias("start_date_label"),
    )
)
