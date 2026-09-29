from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") >= "2023-08-22") | (F.col("method") == "bank_transfer"))
    .select("paid_at", "order_id", F.upper("method").alias("method_upper"))
)
