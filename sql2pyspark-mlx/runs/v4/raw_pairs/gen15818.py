from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") > 1550) & ~F.col("status").isin(["FAILED", "SUCCESS", "REFUNDED"]))
    .select("id", "method", "status", "paid_at")
    .orderBy("paid_at")
)
