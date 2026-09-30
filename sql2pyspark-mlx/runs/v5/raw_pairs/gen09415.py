from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") >= "2024-07-08")
    .groupBy(
        "merchant",
        F.when(F.col("txn_type").isin(["DEBIT", "REFUND"]), "core").otherwise("other").alias("txn_type_group"),
    )
    .agg(
        F.sum(F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0)).alias("credit_count"),
    )
)
