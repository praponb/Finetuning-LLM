from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("account_type").orderBy("transaction_txn_time", "id")
w3 = Window.orderBy("balance", "id")
w4 = Window.partitionBy("customer_id").orderBy(F.col("balance").desc())
w5 = Window.partitionBy("status").orderBy("balance", "id")

filtered = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2024-02-13").cast("date")) & F.col("status").isNotNull())
    .select("*", F.trim("status").alias("clean_status"))
)

with_latest = (
    filtered
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.txn_type"),
        F.col("c.txn_time").alias("transaction_txn_time"),
    )
)

scored = (
    with_latest
    .withColumn("prev_balance", F.lag("balance", 1).over(w2))
    .withColumn("balance_bucket", F.ntile(3).over(w3))
    .withColumn("balance_dense_rank", F.dense_rank().over(w4))
    .withColumn("balance_bucket_2", F.ntile(4).over(w5))
)

result = (
    scored
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        "clean_status",
        "prev_balance",
        "balance_dense_rank",
        "balance_bucket_2",
    )
    .orderBy(F.col("balance_dense_rank").desc())
)
