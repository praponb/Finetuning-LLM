from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") == "Grab") & (F.col("txn_time") < "2025-10-10"))
    .groupBy(F.quarter("txn_time").alias("txn_quarter"))
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.max("txn_time").alias("latest_txn_time"),
    )
    .filter(F.col("avg_amount") >= 67)
    .orderBy("avg_amount")
)
