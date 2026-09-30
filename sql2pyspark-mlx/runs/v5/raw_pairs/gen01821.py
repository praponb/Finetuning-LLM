from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") <= 780) & ((F.col("amount") <= 1250) | (F.col("paid_at") >= "2025-08-01")))
    .groupBy("method", "status")
    .agg(F.min("amount").alias("min_amount"))
    .filter(F.col("min_amount") > 1220)
    .orderBy("method")
)
