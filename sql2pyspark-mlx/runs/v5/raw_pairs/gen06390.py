from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
)

with_dims = (
    base
    .alias("src")
    .join(df_accounts.alias("p"), F.col("src.account_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.account_type"), F.col("p.opened_date"), F.col("p.status"))
)

joined = (
    with_dims
    .alias("src")
    .join(df_accounts.alias("b"), F.col("src.account_id") == F.col("b.id"), "left")
    .filter(F.col("b.balance") <= 62000)
    .select(
        "src.id",
        "src.account_id",
        "src.txn_type",
        "src.amount",
        "src.txn_time",
        "src.merchant",
        "src.status",
        F.col("b.opened_date").alias("account_opened_date"),
        F.col("b.status").alias("account_status"),
        F.col("b.balance"),
    )
)

with_prev = joined.withColumn("prev_txn_time", F.lag("txn_time").over(w))

with_gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

joined_transactions = (
    sessions_transactions
    .alias("a")
    .join(df_accounts.alias("p2"), F.col("a.account_id") == F.col("p2.id"), "left")
    .select(
        "a.id",
        "a.account_id",
        "a.status",
        "a.account_status",
        "a.is_new_burst",
        F.col("p2.opened_date"),
        F.col("p2.status").alias("account_status_2"),
        F.col("p2.balance").alias("account_balance"),
    )
)

result = (
    joined_transactions
    .groupBy("account_status_2")
    .pivot("account_status", ["FROZEN", "CLOSED"])
    .agg(F.sum("account_balance"))
)
