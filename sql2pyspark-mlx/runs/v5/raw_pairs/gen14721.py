from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy("is_flagged", "merchant")
    .agg(
        F.sum(F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0)).alias("credit_count"),
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_count"),
    )
    .filter(F.col("refund_count") >= 7)
)
