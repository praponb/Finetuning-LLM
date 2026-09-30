from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy(F.col("txn_time").desc(), "id")
w_grp = Window.partitionBy("account_id").orderBy(F.col("seq_no_2").asc_nulls_last())
w3 = Window.partitionBy("account_id").orderBy(F.col("seq_no_2_whole").asc_nulls_first())
w4 = Window.partitionBy("merchant")
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.sum("amount").alias("sum_transaction_amount"))

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_time",
        "merchant",
        "is_flagged",
        F.concat_ws("-", "merchant", "txn_type").alias("merchant_txn_type_key"),
        F.coalesce("merchant", F.lit("N/A")).alias("merchant_clean"),
    )
)

metrics = (
    filtered
    .filter(F.col("txn_time") < "2023-08-01")
    .select(
        "*",
        F.row_number().over(w_recent).alias("seq_no"),
        F.row_number().over(w_recent).alias("seq_no_2"),
    )
)

cleaned = (
    metrics
    .filter((F.col("merchant").eqNullSafe(F.lit("Amazon"))) & F.col("is_flagged"))
    .select(
        "id",
        "account_id",
        "txn_time",
        "merchant",
        "is_flagged",
        "merchant_clean",
        "seq_no",
        "seq_no_2",
        F.floor("seq_no_2").alias("seq_no_2_whole"),
    )
)

metrics_transactions = (
    cleaned
    .select(
        "id",
        "account_id",
        "txn_time",
        "is_flagged",
        "merchant_clean",
        "seq_no",
        "seq_no_2",
        F.cume_dist().over(w_grp).alias("seq_no_2_cume_dist"),
        F.percent_rank().over(w3).alias("seq_no_2_whole_percent_rank"),
        F.stddev_samp("seq_no_2_whole").over(w4).alias("merchant_stddev_samp_seq_no_2_whole"),
    )
)

base = (
    metrics_transactions
    .filter(F.col("txn_time") >= F.to_timestamp(F.lit("2023-09-02 03:00:00")))
    .select(
        "*",
        F.year("txn_time").alias("txn_time_year"),
        F.datediff(F.current_date(), "txn_time").alias("days_since_txn_time"),
    )
)

joined = (
    base
    .alias("e")
    .join(df_accounts.alias("b"), F.col("e.account_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.account_id",
        "e.txn_time",
        "e.seq_no_2_whole_percent_rank",
        "e.merchant_stddev_samp_seq_no_2_whole",
        "e.days_since_txn_time",
        F.col("b.account_type"),
        F.col("b.status"),
    )
)

valid = (
    joined
    .filter((F.col("account_id") == 23) & (~F.col("status").eqNullSafe(F.lit("CLOSED"))))
    .select(
        "*",
        F.when(F.col("status") == "CLOSED", 1).otherwise(0).alias("is_closed"),
        F.coalesce("status", F.lit("N/A")).alias("status_filled"),
        F.coalesce("account_type", F.lit("UNKNOWN")).alias("account_type_filled"),
    )
)

result = (
    valid
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "merchant_stddev_samp_seq_no_2_whole",
        "days_since_txn_time",
        "is_closed",
        "account_type_filled",
        F.col("sum_transaction_amount"),
    )
    .orderBy(F.col("is_closed").asc_nulls_first(), "id")
)
