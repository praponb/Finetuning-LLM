from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.quarter("txn_time").alias("txn_quarter"))
    .agg(
        F.sum(F.when(F.col("txn_type") == "REFUND", F.col("amount")).otherwise(0)).alias("refund_amount"),
        F.sum("amount").alias("total_amount"),
        F.count(F.when(F.col("merchant") == "Starbucks", 1)).alias("starbucks_count"),
    )
    .orderBy(F.col("refund_amount").desc())
)
