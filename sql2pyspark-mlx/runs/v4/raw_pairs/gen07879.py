from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") < "2024-06-01") & (F.col("status") == "FAILED"))
    .select(
        "id",
        "amount",
        "method",
        F.lower("status").alias("status_lower"),
        F.hour("paid_at").alias("paid_at_hour"),
    )
    .orderBy(F.col("amount").desc())
)
