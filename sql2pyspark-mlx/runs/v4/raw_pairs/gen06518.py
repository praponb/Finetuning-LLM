from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["FAILED", "REFUNDED"]) & (F.col("amount") >= 980))
    .select("paid_at", F.to_date("paid_at").alias("paid_at_date"))
    .orderBy("paid_at")
)
