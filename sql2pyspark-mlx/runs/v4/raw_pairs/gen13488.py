from pyspark.sql import functions as F

kept = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") <= F.col("s.opened_date")), "left_semi")
)

result = (
    kept
    .groupBy(
        "status",
        F.when(F.col("balance") >= 63000, "HIGH").otherwise("LOW").alias("balance_tier"),
    )
    .agg(
        F.sum(F.when(F.col("account_type") == "CHECKING", F.col("balance")).otherwise(0)).alias("checking_balance"),
        F.count(F.when(F.col("account_type") == "SAVINGS", 1)).alias("savings_rows"),
        F.avg("balance").alias("avg_balance"),
        F.min("balance").alias("min_balance"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("status") == "OPEN", 1)), F.lit(0)), 2).alias("balance_per_open"),
    )
    .orderBy(F.col("balance_per_open").desc())
)
