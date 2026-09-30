from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "account_type").orderBy(F.col("balance").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("transaction_txn_type").orderBy("transaction_amount_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("transaction_txn_type").orderBy("opened_date_plus_72d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

valid = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-05-28").cast("date")) & F.col("status").isNotNull())
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.date_add("opened_date", 72).alias("opened_date_plus_72d"),
        F.coalesce("status", F.lit("DIRECT")).alias("resolved_status"),
    )
)

not_flagged = (
    valid
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.merchant").isin(["Grab", "Lazada"])), "left_anti")
)

ranked = (
    not_flagged
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

with_latest = (
    ranked
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.opened_date",
        "s.opened_date_plus_72d",
        "s.resolved_status",
        "s.row_num",
        F.col("c.txn_type").alias("transaction_txn_type"),
        F.col("c.amount").alias("transaction_amount"),
    )
)

with_latest_accounts = (
    with_latest
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount").alias("transaction_amount_2"))
)

with_windows = (
    with_latest_accounts
    .select(
        "id",
        "opened_date",
        "opened_date_plus_72d",
        "transaction_amount",
        F.last("transaction_txn_type", ignorenulls=True).over(w3).alias("last_known_transaction_txn_type"),
        F.sum("transaction_amount").over(w4).alias("running_transaction_amount"),
    )
)

with_last = (
    with_windows
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount").alias("transaction_amount_2"), F.col("c.merchant"))
)

result = (
    with_last
    .select(
        "id",
        "opened_date_plus_72d",
        "transaction_amount",
        "last_known_transaction_txn_type",
        "running_transaction_amount",
        "transaction_amount_2",
    )
)
