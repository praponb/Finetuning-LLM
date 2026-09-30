from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "paid_at",
        "status",
        "method",
        F.col("paid_at").cast("date").alias("paid_at_day"),
        F.date_trunc("YEAR", "paid_at").alias("paid_at_period"),
    )
    .orderBy("paid_at_period")
)
