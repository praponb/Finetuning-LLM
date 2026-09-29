from pyspark.sql import functions as F

result = (
    df_payments
    .select("id", "paid_at", "status", "method", F.trim("status").alias("clean_status"))
)
