from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("transaction_txn_type").orderBy(F.col("amount").desc_nulls_first())
w4 = Window.partitionBy("transaction_txn_type").orderBy(F.col("amount").desc(), "id")
w5 = Window.partitionBy("transaction_txn_type").orderBy("row_num", "id")
w6 = Window.partitionBy("transaction_txn_type").orderBy("row_num", "id").rowsBetween(-5, -1)
w_recent = Window.orderBy(F.col("amount").desc_nulls_first())
w_ordered = Window.partitionBy("transaction_txn_type").orderBy(F.col("amount_bucket").desc(), "id")

with_latest = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.customer_id", "s.opened_date", "s.status", F.col("c.amount"))
)

with_last = (
    with_latest
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.status", "s.amount", F.col("c.txn_type"))
)

with_last_accounts = (
    with_last
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.amount", F.col("c.txn_type").alias("transaction_txn_type"))
)

deduped = (
    with_last_accounts
    .withColumn("row_num", F.rank().over(w3))
    .filter(F.col("row_num") == 1)
)

eligible = (
    deduped
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (~F.col("ch.txn_type").isin(["CREDIT", "FEE"])), "left_anti")
)

with_windows = (
    eligible
    .filter(F.col("row_num") <= 1)
    .select(
        "*",
        F.ntile(3).over(w4).alias("amount_bucket"),
        F.lag("amount", 1).over(w5).alias("prev_amount"),
        F.avg("amount").over(w6).alias("amount_avg_5_prior"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "transaction_txn_type",
        "row_num",
        F.dense_rank().over(w_recent).alias("amount_dense_rank"),
        F.row_number().over(w_ordered).alias("seq_no"),
    )
)

result = (
    metrics
    .select("id", "transaction_txn_type", "amount_dense_rank", "seq_no")
    .orderBy(F.col("id").asc_nulls_first(), "seq_no")
)
