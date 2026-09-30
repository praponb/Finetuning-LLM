from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([11, 12, 21, 30]) & (F.col("paid_at") < "2024-06-01"))
    .withColumn("method_length", F.length("method"))
    .orderBy(F.col("id").desc())
    .limit(10)
)
