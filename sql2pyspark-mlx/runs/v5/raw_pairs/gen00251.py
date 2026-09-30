from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([8, 25, 35]))
    .groupBy("is_flagged", F.quarter("txn_time").alias("txn_quarter"))
    .agg(
        F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("amount")).otherwise(0)).alias("credit_amount"),
    )
    .filter(F.col("credit_amount") > 63)
    .select("is_flagged", "txn_quarter", "credit_amount")
    .orderBy("txn_quarter", "is_flagged")
    .limit(20)
)
