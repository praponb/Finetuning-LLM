from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("txn_type")
w3 = Window.partitionBy("txn_type", "merchant").orderBy(F.col("amount").asc_nulls_last())
w4 = Window.partitionBy("merchant", "account_type").orderBy(F.col("txn_time").desc())
w5 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(-5, Window.currentRow)
w_running = Window.orderBy(F.col("txn_time").desc(), "id")
w7 = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")
w8 = Window.orderBy("running_amount", "id")
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.max("amount").alias("max_transaction_amount"))

joined = (
    df_transactions
    .alias("e")
    .join(df_accounts.alias("a"), F.col("e.account_id") == F.col("a.id"), "inner")
    .select("e.*", F.col("a.balance"))
)

scored = (
    joined
    .withColumn("running_amount", F.sum("amount").over(w))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w2)), 4))
    .withColumn("amount_percent_rank", F.percent_rank().over(w3))
)

joined_transactions = (
    scored
    .alias("e")
    .join(df_accounts.alias("p"), F.col("e.account_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.balance").alias("account_balance"), F.col("p.account_type"))
)

ranked = (
    joined_transactions
    .withColumn("row_num", F.rank().over(w4))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched = (
    ranked
    .withColumn("balance_avg_5", F.avg("balance").over(w5))
    .withColumn("seq_no", F.row_number().over(w_running))
)

scored_transactions = (
    enriched
    .select(
        "*",
        F.row_number().over(w7).alias("event_seq"),
        F.lag("amount", 1).over(w8).alias("prev_amount"),
    )
)

result = (
    scored_transactions
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "is_flagged",
        "balance",
        "amount_share",
        "account_balance",
        "account_type",
        F.col("max_transaction_amount"),
    )
)
