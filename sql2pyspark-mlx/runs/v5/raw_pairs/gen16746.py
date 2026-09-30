from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") < 510)
    .select("paid_at", "method", "amount", F.lower("status").alias("status_lower"))
)
