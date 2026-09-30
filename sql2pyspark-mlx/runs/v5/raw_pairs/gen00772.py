from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("txn_time") >= "2025-03-02"))
    .groupBy("txn_type")
    .agg(
        F.min("txn_time").alias("first_txn_time"),
        F.sum(F.when(F.col("txn_type") == "REFUND", 1).otherwise(0)).alias("refund_count"),
        F.max("amount").alias("max_amount"),
    )
    .orderBy(F.col("refund_count").desc(), "txn_type")
    .limit(5)
)
