from pyspark.sql import functions as F

result = (
    df_payments
    .filter(((F.col("amount") >= 970) & (F.col("method") == "bank_transfer")) | (F.col("status") == "FAILED"))
    .select("paid_at", "amount", "status", F.to_date("paid_at").alias("paid_at_date"))
    .orderBy("status", "paid_at_date")
)
