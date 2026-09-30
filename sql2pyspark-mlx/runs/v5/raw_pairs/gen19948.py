from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("account_type_length").asc_nulls_last(), "id")
w_recent = Window.partitionBy("status", "customer_id").orderBy("balance", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("customer_id", "account_type").orderBy(F.col("account_type_length").desc(), "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("customer_id").orderBy(F.col("distinct_txn_types").desc())

filtered = (
    df_accounts
    .filter(F.col("opened_date") >= "2023-11-01")
    .select("*", F.length("account_type").alias("account_type_length"))
)

latest = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

kept = (
    latest
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") < F.col("s.opened_date")), "left_anti")
)

with_windows = (
    kept
    .filter(F.col("customer_id").isNull())
    .withColumn("last4_sum_account_type_length", F.sum("account_type_length").over(w_recent))
    .withColumn("last4_min_account_type_length", F.min("account_type_length").over(w_recent))
    .withColumn("last4_count_account_type_length", F.count("account_type_length").over(w_recent))
    .withColumn("event_seq", F.row_number().over(w3))
)

with_latest = (
    with_windows
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.txn_time").alias("transaction_txn_time"),
        F.col("c.txn_type"),
    )
)

kept_accounts = (
    with_latest
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (~F.col("ch.is_flagged")), "left_semi")
)

kept_accounts_2 = (
    kept_accounts
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") <= F.col("s.transaction_txn_time")), "left_anti")
)

result = (
    kept_accounts_2
    .groupBy(
        "customer_id",
        F.when(F.col("balance") >= 81000, "PREMIUM").otherwise("STANDARD").alias("balance_tier"),
    )
    .agg(
        F.min("transaction_txn_time").alias("first_seen"),
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_rows"),
        F.countDistinct("txn_type").alias("distinct_txn_types"),
    )
    .filter(F.col("distinct_txn_types") > 1)
    .withColumn("distinct_txn_types_dense_rank", F.dense_rank().over(w5))
    .orderBy(F.col("customer_id").desc_nulls_first())
)
