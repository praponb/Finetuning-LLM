from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") >= "2023-10-01")
    .select("id", "amount", "order_id", F.length("method").alias("method_length"))
    .orderBy(F.col("id").desc())
)
