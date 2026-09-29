from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(6, 14) | (F.col("paid_at") >= "2025-06-01"))
    .select("amount", "order_id", F.hour("paid_at").alias("paid_at_hour"))
    .orderBy(F.col("amount").desc())
)
