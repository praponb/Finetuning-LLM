from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") >= "2023-07-01")
    .groupBy("method", "order_id")
    .agg(
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_count"),
        F.max("amount").alias("max_amount"),
    )
    .filter(F.col("max_amount") >= 410)
    .select("method", "order_id", "bank_transfer_count", "max_amount")
    .orderBy("max_amount", F.col("order_id").desc())
)
