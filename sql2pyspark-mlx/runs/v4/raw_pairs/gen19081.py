from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["REFUND", "FEE", "DEBIT"]))
    .groupBy("merchant")
    .agg(
        F.min("txn_time").alias("min_txn_time"),
        F.count(F.when(F.col("merchant") == "7-Eleven", 1)).alias("7_eleven_count"),
        F.sum(F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0)).alias("credit_count"),
    )
)
