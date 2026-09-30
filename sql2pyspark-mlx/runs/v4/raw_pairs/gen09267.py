from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["bank_transfer", "paypal", "cash"]) & F.col("status").isin(["FAILED", "REFUNDED", "SUCCESS"]) & (F.col("amount") < 540))
    .select("id", "amount", "order_id", F.to_date("paid_at").alias("paid_at_date"))
)
