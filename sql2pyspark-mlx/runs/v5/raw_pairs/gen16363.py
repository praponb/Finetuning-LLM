from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") >= "2024-03-27") & (F.to_date("paid_at") >= "2024-03-01"))
    .select("amount", F.length("method").alias("method_length"))
)
