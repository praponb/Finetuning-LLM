from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("txn_type").isin(["REFUND", "DEBIT"]), "tier_1").otherwise("other").alias("txn_type_group"),
        F.quarter("txn_time").alias("txn_quarter"),
    )
    .agg(
        F.max("txn_time").alias("latest_txn_time"),
        F.countDistinct("amount").alias("unique_txn_types"),
        F.sum(F.when(F.col("txn_type") == "DEBIT", 1).otherwise(0)).alias("debit_count"),
    )
    .orderBy(F.col("txn_quarter").desc())
)
