from pyspark.sql import functions as F

kept = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") <= F.col("s.opened_date")), "left_semi")
)

result = (
    kept
    .groupBy(
        F.when(F.col("balance") >= 62000, "PREMIUM").otherwise("STANDARD").alias("balance_tier"),
    )
    .agg(
        F.size(F.collect_set("account_type")).alias("num_account_types"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_rows"),
    )
    .filter(F.col("num_account_types") >= 1)
)
