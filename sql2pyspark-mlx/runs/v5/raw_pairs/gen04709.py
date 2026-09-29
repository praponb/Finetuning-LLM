from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").isNotNull())
    .groupBy("account_id")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.count("amount").alias("amount_count"),
        F.sum(F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0)).alias("credit_count"),
    )
    .filter(F.col("credit_count") >= 34)
    .drop("credit_count")
    .orderBy("amount_range", "account_id")
    .limit(5)
)
