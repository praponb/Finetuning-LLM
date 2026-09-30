from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id", "status").orderBy("txn_time", "id").rowsBetween(-6, Window.currentRow)
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.avg("amount").alias("avg_transaction_amount"))

cleaned = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("merchant") == "Shell"))
    .select("*", F.date_format("txn_time", "yyyy-MM").alias("txn_time_label"))
)

with_dims = (
    cleaned
    .alias("e")
    .join(df_accounts.alias("d"), F.col("e.account_id") == F.col("d.id"), "inner")
    .filter(F.col("d.status") != "FROZEN")
    .select("e.*", F.col("d.account_type"))
)

joined = (
    with_dims
    .alias("a")
    .join(df_accounts.alias("d"), F.col("a.account_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.account_id",
        "a.txn_type",
        "a.amount",
        "a.txn_time",
        "a.txn_time_label",
        "a.account_type",
        F.col("d.status"),
        F.col("d.balance"),
    )
)

scored = (
    joined
    .withColumn("next_balance", F.lead("balance", 1).over(w))
    .withColumn("cumulative_amount", F.sum("amount").over(w2))
    .withColumn("balance_avg_6", F.avg("balance").over(w3))
)

joined_transactions = (
    scored
    .alias("s")
    .join(df_accounts.alias("d"), F.col("s.account_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.account_id",
        "s.txn_time",
        "s.next_balance",
        "s.balance_avg_6",
        F.col("d.balance").alias("account_balance"),
        F.col("d.status").alias("account_status"),
    )
)

pivoted = (
    joined_transactions
    .groupBy("account_id")
    .pivot("account_status", ["CLOSED", "FROZEN", "OPEN"])
    .agg(F.sum("account_balance").alias("value_sum"))
    .select(
        "account_id",
        F.col("CLOSED").alias("closed"),
        F.col("FROZEN").alias("frozen"),
        F.col("OPEN").alias("open"),
    )
)

result = (
    pivoted
    .join(amount_by_account_id, on="account_id", how="left")
    .select("account_id", "frozen", "open", F.col("avg_transaction_amount"))
    .orderBy("account_id", "frozen")
)
