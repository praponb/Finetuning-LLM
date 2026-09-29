from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.quarter("txn_time").alias("txn_quarter"), "txn_type")
    .agg(
        F.sum(F.when(F.col("txn_type") == "DEBIT", 1).otherwise(0)).alias("debit_count"),
    )
    .filter(F.col("debit_count") > 23)
    .select("txn_quarter", "txn_type", "debit_count")
    .orderBy(F.col("txn_quarter").desc(), "txn_type")
    .limit(20)
)
