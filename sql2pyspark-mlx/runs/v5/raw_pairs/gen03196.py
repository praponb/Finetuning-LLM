from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

filtered = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .withColumn("balance_rounded", F.round("balance", 0))
    .withColumn("is_frozen", F.when(F.col("status") == "FROZEN", 1).otherwise(0))
    .withColumn("status_clean", F.coalesce("status", F.lit("NONE")))
)

with_latest = (
    filtered
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.account_type",
        "s.balance",
        "s.status",
        "s.status_clean",
        F.col("c.txn_type").alias("transaction_txn_type"),
    )
)

with_last = (
    with_latest
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.account_type",
        "s.status_clean",
        F.col("c.merchant").alias("transaction_merchant"),
    )
)

with_last_accounts = (
    with_last
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.merchant"), F.col("c.amount"))
)

stats = (
    with_last_accounts
    .groupBy(
        F.when(F.col("amount") >= 3900, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.count(F.when(F.col("merchant") == "Shell", 1)).alias("shell_count"),
        F.count("*").alias("num_rows"),
        F.size(F.collect_set("merchant")).alias("num_merchants"),
    )
)

result = (
    stats
    .select("amount_tier", "shell_count")
    .orderBy(F.col("amount_tier").desc())
)
