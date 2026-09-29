from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("merchant") == "Shell", 1).otherwise(0)).alias("shell_count"),
        F.sum(F.when(F.col("txn_type") == "FEE", F.col("amount")).otherwise(0)).alias("fee_amount"),
    )
    .orderBy("txn_type")
    .limit(3)
)
