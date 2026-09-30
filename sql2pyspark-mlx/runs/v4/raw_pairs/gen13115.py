from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id", "is_flagged")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.countDistinct("amount").alias("unique_txn_types"),
        F.max("txn_time").alias("latest_txn_time"),
        F.count("amount").alias("amount_count"),
    )
    .filter(F.col("amount_count") > 1)
    .drop("amount_count")
    .orderBy("unique_txn_types")
)
