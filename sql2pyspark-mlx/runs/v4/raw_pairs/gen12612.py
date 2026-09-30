from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("account_id") == 3) & ((F.col("merchant") == "Shell") | F.col("merchant").isin(["Starbucks", "Amazon"])))
    .groupBy(
        F.when(F.col("txn_type").isin(["DEBIT", "REFUND"]), "priority").otherwise("other").alias("txn_type_group"),
    )
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_count"),
    )
)
