from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id")
w2 = Window.partitionBy("status").orderBy("balance", "id")
w3 = Window.partitionBy("account_type").orderBy("opened_date", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w5 = Window.orderBy("amount", "id")
w6 = Window.partitionBy("account_type").orderBy("amount", "id").rowsBetween(-6, -1)

scored = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.lead("balance", 1).over(w).alias("next_balance"),
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w2).alias("row_seq"),
        F.lag("balance", 2, 0).over(w3).alias("prev_balance"),
        F.ntile(10).over(w2).alias("balance_bucket"),
    )
)

with_latest = (
    scored
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.account_type",
        "s.next_balance",
        "s.seq_no",
        "s.row_seq",
        "s.balance_bucket",
        F.col("c.amount"),
        F.col("c.merchant").alias("transaction_merchant"),
    )
)

enriched = (
    with_latest
    .select(
        "id",
        "next_balance",
        "seq_no",
        "row_seq",
        "balance_bucket",
        F.lag("seq_no", 1).over(w5).alias("prev_seq_no"),
        F.avg("row_seq").over(w6).alias("row_seq_avg_6_prior"),
    )
)

with_last = (
    enriched
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.next_balance",
        "s.balance_bucket",
        "s.row_seq_avg_6_prior",
        F.col("c.amount").alias("transaction_amount"),
        F.col("c.txn_type"),
    )
)

result = (
    with_last
    .select(
        "id",
        "balance_bucket",
        "row_seq_avg_6_prior",
        "transaction_amount",
        "txn_type",
    )
    .orderBy(F.col("txn_type").desc_nulls_last())
)
