from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([27, 32]) & F.col("status").isin(["SUCCESS", "REFUNDED"]))
    .select("id", "status", "method", "paid_at")
)
