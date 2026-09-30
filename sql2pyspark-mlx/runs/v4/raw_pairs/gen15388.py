from pyspark.sql import functions as F

recent_transactions = df_transactions.filter(F.col("merchant") == "7-Eleven")

is_flagged_summary = (
    recent_transactions
    .groupBy("is_flagged", "txn_type")
    .agg(
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.countDistinct("amount").alias("unique_txn_types"),
    )
)

result = (
    is_flagged_summary
    .select("is_flagged", "txn_type", "merchant_list", "unique_txn_types")
)
