from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNull() & (F.col("method") != "card") & (F.col("paid_at") >= "2025-01-11"))
    .withColumn("amount_int", F.col("amount").cast("int"))
    .orderBy("amount")
)
