from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc_nulls_last(), "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w3 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w5 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("account_id", "txn_type").orderBy(F.col("balance").desc(), "id")
w7 = Window.partitionBy("txn_type", "status").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("session_start_flag", "id")

deduped = (
    df_transactions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_gaps = (
    deduped
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w2) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("is_new_burst").over(w3))

enriched = (
    islands
    .alias("a")
    .join(df_accounts.alias("p2"), F.col("a.account_id") == F.col("p2.id"), "inner")
    .filter(F.col("p2.account_type") == "CREDIT")
    .select(
        "a.id",
        "a.account_id",
        "a.txn_type",
        "a.txn_time",
        "a.merchant",
        "a.rn",
        F.col("p2.opened_date"),
        F.col("p2.balance"),
        F.col("p2.status"),
    )
)

gaps = (
    enriched
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w4) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_transactions = gaps.withColumn("island_id", F.sum("session_start_flag").over(w5))

deduped_transactions = (
    sessions_transactions
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped_transactions
    .select(
        "id",
        "account_id",
        "txn_type",
        "merchant",
        "opened_date",
        "balance",
        "status",
        "session_start_flag",
        "island_id",
        "latest_rank",
        F.sum("session_start_flag").over(w7).alias("running_session_start_flag"),
        F.ntile(10).over(w8).alias("session_start_flag_bucket"),
    )
)
