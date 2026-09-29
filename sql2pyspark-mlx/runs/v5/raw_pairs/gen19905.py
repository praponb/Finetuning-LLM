from pyspark.sql import functions as F

with_dims = (
    df_transactions
    .alias("a")
    .join(df_accounts.alias("d"), F.col("a.account_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.account_id",
        "a.txn_type",
        "a.txn_time",
        "a.is_flagged",
        F.col("d.opened_date"),
        F.col("d.account_type"),
        F.col("d.balance"),
    )
)

result = (
    with_dims
    .withColumn("week_start", F.date_trunc("WEEK", "txn_time"))
    .groupingSets(
        [["account_id", "week_start"], ["account_id"]],
        "account_id",
        "week_start",
    )
    .agg(
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.avg("balance").alias("avg_balance"),
        F.min("balance").alias("min_balance"),
        F.min("txn_time").alias("first_seen"),
    )
)
