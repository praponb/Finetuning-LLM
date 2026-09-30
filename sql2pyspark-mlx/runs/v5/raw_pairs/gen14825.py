from pyspark.sql import functions as F

recent_transactions = (
    df_transactions
    .filter(((F.col("txn_type") == "FEE") & F.col("merchant").isin(["7-Eleven", "Shell"])) | F.col("account_id").isin([8, 23]))
)

transactions_stats = (
    recent_transactions
    .groupBy("is_flagged", "txn_type")
    .agg(
        F.sum(F.when(F.col("txn_type") == "FEE", 1).otherwise(0)).alias("fee_count"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
    )
)

result = (
    transactions_stats
    .select("is_flagged", "txn_type", "fee_count", "merchant_list")
    .orderBy("txn_type")
)
