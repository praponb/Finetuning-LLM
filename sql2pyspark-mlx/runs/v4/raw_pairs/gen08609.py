from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") >= 830)
    .select("id", "amount", "status", "paid_at")
    .orderBy("paid_at")
)
