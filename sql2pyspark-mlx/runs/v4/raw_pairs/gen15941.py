from pyspark.sql import functions as F

valid = (
    df_transactions
    .filter(F.col("account_id").isin([28, 38]))
    .select(
        "id",
        "account_id",
        "txn_type",
        "merchant",
        "is_flagged",
        F.round("amount", 0).alias("amount_rounded"),
    )
)

result = (
    valid
    .groupBy("txn_type")
    .agg(
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.percentile_approx("amount_rounded", [0.25, 0.5, 0.75]).alias("amount_rounded_quartiles"),
        F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_rows"),
    )
)
