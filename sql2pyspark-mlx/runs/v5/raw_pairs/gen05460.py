from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .pivot("status", ["FAILED", "SUCCESS", "REFUNDED"])
    .agg(F.avg("amount").alias("amount"))
    .select(
        "method",
        F.col("FAILED").alias("failed"),
        F.col("SUCCESS").alias("success"),
        F.col("REFUNDED").alias("refunded"),
    )
)
