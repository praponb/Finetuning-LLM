from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("txn_time", "id")
w2 = Window.partitionBy("status").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.sum("amount").alias("sum_transaction_amount"))

valid = (
    df_transactions
    .filter(F.col("txn_time") >= F.lit("2023-07-03 16:00:00").cast("timestamp"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        F.concat("merchant", F.lit(" "), "txn_type").alias("merchant_txn_type"),
    )
)

with_dims = (
    valid
    .alias("e")
    .join(df_accounts.alias("d"), F.col("e.account_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.balance"), F.col("d.status"), F.col("d.opened_date"))
)

with_dims_transactions = (
    with_dims
    .alias("s")
    .join(df_accounts.alias("b"), F.col("s.account_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.account_type"), F.col("b.balance").alias("account_balance"))
)

with_dims_transactions_2 = (
    with_dims_transactions
    .alias("a")
    .join(df_accounts.alias("d"), F.col("a.account_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.balance").alias("account_balance_2"))
)

gaps = (
    with_dims_transactions_2
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        "merchant_txn_type",
        "balance",
        "status",
        F.col("sum_transaction_amount"),
    )
    .orderBy(F.col("txn_time").asc_nulls_last())
)
