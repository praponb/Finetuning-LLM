from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.quarter("txn_time").alias("txn_quarter"))
    .agg(
        F.sum(F.when(F.col("merchant") == "Shell", 1).otherwise(0)).alias("shell_count"),
    )
    .filter(F.col("shell_count") >= 40)
    .orderBy("txn_quarter")
)
