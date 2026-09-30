from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy(F.col("balance_whole").desc(), "id")
w_running = Window.partitionBy("account_type").orderBy("balance_whole", "id")
w3 = Window.partitionBy("customer_id", "status").orderBy("balance_whole")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

valid = (
    df_accounts
    .filter(F.col("opened_date") > F.lit("2023-04-07").cast("date"))
    .withColumn("balance_whole", F.floor("balance"))
    .withColumn("resolved_status", F.coalesce("status", F.lit("OTHER")))
)

ranked = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

enriched = (
    ranked
    .select(
        "id",
        "customer_id",
        "account_type",
        "status",
        "balance_whole",
        "resolved_status",
        F.lag("balance_whole").over(w_running).alias("prev_balance_whole"),
        F.lead("balance").over(w_running).alias("next_balance"),
        F.row_number().over(w_running).alias("seq_no"),
        F.rank().over(w3).alias("balance_whole_rank"),
    )
)

with_latest = (
    enriched
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.balance_whole",
        "s.prev_balance_whole",
        "s.seq_no",
        F.col("c.amount"),
        F.col("c.txn_time"),
    )
)

enriched_accounts = (
    with_latest
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.email"))
)

with_latest_accounts = (
    enriched_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.prev_balance_whole",
        "s.seq_no",
        "s.amount",
        F.col("c.txn_type").alias("transaction_txn_type"),
    )
)

result = (
    with_latest_accounts
    .select(
        "id",
        "prev_balance_whole",
        "seq_no",
        "amount",
        "transaction_txn_type",
        F.when(F.col("transaction_txn_type").rlike("^(FEE|CREDIT)$"), "OTHER_ZONE").when(F.col("transaction_txn_type").rlike("^(DEBIT|REFUND)$"), "PRIORITY").otherwise("GROUP_B").alias("transaction_txn_type_zone"),
    )
)
