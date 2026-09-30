from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["SUCCESS", "FAILED"]))
    .groupBy("method")
    .agg(
        F.max("paid_at").alias("latest_paid_at"),
        F.min("amount").alias("min_amount"),
    )
)
