from pyspark.sql import functions as F

remaining_keys = (
    df_transactions
    .select("txn_type", "account_id")
    .intersect(
        df_transactions
        .filter(F.col("txn_type") == "REFUND")
        .select("txn_type", "account_id")
    )
)

reconciled = (
    df_transactions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")) & (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(F.date_trunc("QUARTER", "txn_time").alias("quarter_start"), "account_id")
    .agg(
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.countDistinct("account_id").alias("distinct_account_ids"),
    )
    .orderBy("account_id")
)
