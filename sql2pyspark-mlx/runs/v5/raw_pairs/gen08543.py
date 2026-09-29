from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .agg(
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_count"),
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_count"),
    )
    .orderBy(F.col("fee_count").desc())
)
