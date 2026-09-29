from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant")
    .agg(
        F.max("txn_time").alias("latest_txn_time"),
        F.sum(F.when(F.col("merchant") == "Shell", F.col("amount")).otherwise(0)).alias("shell_amount"),
    )
    .filter(F.col("shell_amount") < 29)
    .orderBy(F.col("shell_amount").desc(), F.col("merchant").desc())
    .limit(50)
)
