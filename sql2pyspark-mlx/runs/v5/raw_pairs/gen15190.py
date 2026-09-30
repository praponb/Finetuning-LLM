from pyspark.sql import functions as F

not_flagged = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") >= F.col("s.opened_date")), "left_semi")
)

result = (
    not_flagged
    .groupBy(
        F.when(F.col("balance") >= 62000, "LARGE").otherwise("SMALL").alias("balance_tier"),
        F.date_trunc("YEAR", "opened_date").alias("year_start"),
        "status",
    )
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.count(F.when(F.col("account_type") == "SAVINGS", 1)).alias("savings_rows"),
        F.sum("balance").alias("total_balance"),
        F.sum(F.when(F.col("status") == "OPEN", F.col("balance")).otherwise(0)).alias("open_balance"),
    )
    .orderBy(F.col("year_start").desc_nulls_first())
)
