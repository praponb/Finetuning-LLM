from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("merchant").orderBy(F.col("opened_date").desc(), "id")
w3 = Window.partitionBy("merchant").orderBy("transaction_txn_time", "id")
w4 = Window.partitionBy("merchant").orderBy("transaction_txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("merchant").orderBy("session_no")
w6 = Window.partitionBy("merchant").orderBy("session_no", "id").rowsBetween(-5, -1)
w7 = Window.partitionBy("merchant").orderBy("transaction_txn_time", "id").rowsBetween(-2, Window.currentRow)
w_recent = Window.orderBy("session_no_cume_dist", "id").rowsBetween(-2, Window.currentRow)

valid = (
    df_accounts
    .filter(F.col("opened_date") >= F.lit("2023-06-28").cast("date"))
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
        F.coalesce("status", F.lit("N/A")).alias("status_clean"),
    )
)

latest_match = (
    valid
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.balance",
        "s.opened_date",
        "s.status",
        "s.balance_filled",
        "s.status_clean",
        F.col("c.txn_time").alias("transaction_txn_time"),
        F.col("c.merchant"),
    )
)

with_last = (
    latest_match
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.transaction_txn_time")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_time").alias("transaction_txn_time_2"))
)

not_flagged = (
    with_last
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") <= F.col("s.opened_date")), "left_anti")
)

deduped = (
    not_flagged
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "opened_date",
        "balance_filled",
        "transaction_txn_time",
        "merchant",
        "transaction_txn_time_2",
    )
)

gaps = (
    deduped
    .withColumn("new_group_flag", F.when(F.lag("transaction_txn_time").over(w3).isNull() | (F.col("transaction_txn_time") > F.lag("transaction_txn_time").over(w3) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_accounts = (
    gaps
    .select(
        "id",
        "transaction_txn_time",
        "merchant",
        "transaction_txn_time_2",
        F.sum("new_group_flag").over(w4).alias("session_no"),
    )
)

enriched = (
    sessions_accounts
    .filter(F.to_date("transaction_txn_time_2") >= "2025-02-01")
    .select(
        "id",
        "transaction_txn_time",
        F.cume_dist().over(w5).alias("session_no_cume_dist"),
        F.avg("session_no").over(w6).alias("session_no_avg_5_prior"),
        F.avg("session_no").over(w7).alias("session_no_avg_2"),
    )
)

result = (
    enriched
    .select(
        "id",
        "session_no_avg_2",
        F.count("session_no_avg_5_prior").over(w_recent).alias("last3_count_session_no_avg_5_prior"),
        F.max("session_no_avg_5_prior").over(w_recent).alias("last3_max_session_no_avg_5_prior"),
        F.avg("session_no_avg_5_prior").over(w_recent).alias("last3_avg_session_no_avg_5_prior"),
    )
)
