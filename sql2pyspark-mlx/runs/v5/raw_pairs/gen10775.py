from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["REFUNDED", "SUCCESS"]))
    .select(
        "id",
        "paid_at",
        F.date_format("paid_at", "yyyy-MM-dd").alias("paid_at_label"),
        F.lower("status").alias("status_lower"),
    )
)
