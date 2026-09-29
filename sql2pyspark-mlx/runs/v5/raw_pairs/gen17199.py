from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("opened_date").desc(), "id")
w2 = Window.partitionBy("merchant").orderBy(F.col("balance").desc())
w3 = Window.partitionBy("status").orderBy("txn_time", "id")
w4 = Window.partitionBy("status").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy("txn_time", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

joined = (
    df_transactions
    .alias("a")
    .join(df_accounts.alias("p2"), F.col("a.account_id") == F.col("p2.id"), "inner")
    .select(
        "a.*",
        F.col("p2.balance"),
        F.col("p2.opened_date"),
        F.col("p2.account_type"),
    )
)

enriched = (
    joined
    .alias("a")
    .join(df_accounts.alias("p2"), F.col("a.account_id") == F.col("p2.id"), "left")
    .select("a.*", F.col("p2.balance").alias("account_balance"))
)

deduped = (
    enriched
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched_transactions = (
    deduped
    .alias("e")
    .join(df_accounts.alias("d"), F.col("e.account_id") == F.col("d.id"), "left")
    .filter(F.col("d.status") != "CLOSED")
    .select(
        "e.*",
        F.col("d.status"),
        F.col("d.opened_date").alias("account_opened_date"),
        F.col("d.account_type").alias("account_account_type"),
    )
)

latest = (
    enriched_transactions
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
)

flagged = (
    latest
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w3).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w3) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("session_start_flag").over(w4))

enriched_transactions_2 = (
    islands
    .alias("s")
    .join(df_accounts.alias("p"), F.col("s.account_id") == F.col("p.id"), "inner")
    .select(
        "s.*",
        F.col("p.status").alias("account_status"),
        F.col("p.account_type").alias("account_account_type_2"),
    )
)

result = (
    enriched_transactions_2
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "balance",
        "session_start_flag",
        "burst_id",
        "account_account_type_2",
        F.min("balance").over(w5).alias("last7_min_balance"),
        F.avg("balance").over(w5).alias("last7_avg_balance"),
        F.count("balance").over(w5).alias("last7_count_balance"),
        F.max("balance").over(w5).alias("last7_max_balance"),
        F.max("balance").over(w6).alias("balance_max_30d"),
    )
)
