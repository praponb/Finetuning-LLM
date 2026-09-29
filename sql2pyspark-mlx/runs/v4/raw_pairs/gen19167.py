from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") == "SUCCESS") & (F.col("amount") < 1130) & (F.to_date("paid_at") >= "2023-11-09"))
    .groupBy(F.quarter("paid_at").alias("paid_quarter"), "status", "method")
    .agg(
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"),
        F.min("paid_at").alias("min_paid_at"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .filter(F.col("refunded_count") >= 5)
)
