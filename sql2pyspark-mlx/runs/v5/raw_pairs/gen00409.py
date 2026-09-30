from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("days_since_opened_date", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("merchant").orderBy(F.col("transaction_txn_time").asc_nulls_first())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

cleaned = (
    df_accounts
    .filter(F.col("status").isNotNull() & (F.col("balance") <= 71000))
    .select(
        "id",
        "account_type",
        "balance",
        "status",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
        F.coalesce("status", F.lit("OTHER")).alias("status_filled"),
    )
)

ranked = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "balance", "days_since_opened_date", "status_filled")
)

valid = (
    ranked
    .filter(F.col("balance") > 73000)
    .select(
        "*",
        F.col("days_since_opened_date").cast("string").alias("days_since_opened_date_str"),
    )
)

with_last = (
    valid
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.days_since_opened_date",
        F.col("c.merchant"),
        F.col("c.txn_time").alias("transaction_txn_time"),
    )
)

latest = (
    with_last
    .withColumn("rank_in_group", F.rank().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "days_since_opened_date", "merchant", "transaction_txn_time")
)

with_latest = (
    latest
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.transaction_txn_time")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.days_since_opened_date",
        "s.merchant",
        "s.transaction_txn_time",
        F.col("c.txn_time").alias("transaction_txn_time_2"),
        F.col("c.amount").alias("transaction_amount"),
    )
)

result = (
    with_latest
    .select(
        "id",
        "days_since_opened_date",
        "merchant",
        "transaction_txn_time",
        "transaction_txn_time_2",
        "transaction_amount",
    )
    .orderBy(F.col("days_since_opened_date").asc_nulls_first())
)
