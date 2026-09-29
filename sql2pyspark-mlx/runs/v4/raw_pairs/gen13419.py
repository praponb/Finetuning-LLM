from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type", "account_id")
    .agg(
        F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("amount")).otherwise(0)).alias("credit_amount"),
    )
    .orderBy("txn_type")
)
