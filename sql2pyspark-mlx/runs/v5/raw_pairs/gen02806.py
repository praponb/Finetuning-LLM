from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([5, 32, 37]) & F.col("account_id").isin([30, 31]) & (F.col("amount") >= 1800))
    .groupBy(
        "is_flagged",
        F.when(F.col("merchant").isin(["Lazada", "Shell"]), "core").otherwise("other").alias("merchant_group"),
    )
    .agg(
        F.sum(F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0)).alias("credit_count"),
    )
)
