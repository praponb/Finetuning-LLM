from pyspark.sql import functions as F

matched_keys = (
    df_transactions
    .select("merchant", "id")
    .subtract(
        df_transactions
        .filter(F.hour("txn_time").between(9, 12))
        .select("merchant", "id")
    )
)

kept_rows = (
    df_transactions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.merchant") == F.col("k.merchant")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(F.date_trunc("QUARTER", "txn_time").alias("quarter_start"), "account_id")
    .agg(
        F.min("txn_time").alias("first_txn_time"),
        F.max("txn_time").alias("last_seen"),
        F.avg(F.when(F.col("txn_type") == "DEBIT", F.col("amount"))).alias("debit_avg_amount"),
    )
    .orderBy("quarter_start")
)
