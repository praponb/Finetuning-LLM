from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("txn_time").asc_nulls_first(), "id")
w2 = Window.partitionBy("account_type").orderBy("txn_time", "id")
w3 = Window.partitionBy("account_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("merchant").orderBy("prev_txn_time", "id")
w5 = Window.partitionBy("merchant").orderBy("prev_txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("account_type").orderBy(F.col("session_id").desc(), "id")
w7 = Window.partitionBy("txn_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("account_id").orderBy("txn_time", "id")
w9 = Window.orderBy("island_id")
w10 = Window.partitionBy("txn_type").orderBy(F.col("txn_time").desc(), "id")

base = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .withColumn("txn_type_lower", F.lower("txn_type"))
)

ranked = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_dims = (
    ranked
    .alias("src")
    .join(df_accounts.alias("d"), F.col("src.account_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.opened_date"), F.col("d.account_type"))
)

prev_events = with_dims.withColumn("prev_txn_time", F.lag("txn_time").over(w2))

gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_transactions = gaps.withColumn("session_id", F.sum("session_start_flag").over(w3))

with_gaps = (
    sessions_transactions
    .withColumn("new_group_flag", F.when(F.lag("prev_txn_time").over(w4).isNull() | (F.col("prev_txn_time") > F.lag("prev_txn_time").over(w4) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_transactions_2 = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w5))

scored = (
    sessions_transactions_2
    .filter(F.col("merchant").isin(["Lazada", "Starbucks", "7-Eleven"]))
    .withColumn("seq_no", F.row_number().over(w6))
    .withColumn("first_known_session_start_flag", F.first("session_start_flag", ignorenulls=True).over(w7))
    .withColumn("prev_session_id", F.lag("session_id", 2).over(w8))
    .withColumn("island_id_cume_dist", F.cume_dist().over(w9))
    .withColumn("seq_no_2", F.row_number().over(w10))
)

enriched = (
    scored
    .alias("e")
    .join(df_accounts.alias("p"), F.col("e.account_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.account_id",
        "e.amount",
        "e.txn_type_lower",
        "e.opened_date",
        "e.prev_txn_time",
        "e.session_start_flag",
        "e.new_group_flag",
        "e.seq_no",
        "e.first_known_session_start_flag",
        F.col("p.opened_date").alias("account_opened_date"),
        F.col("p.balance"),
        F.col("p.status"),
    )
)

result = (
    enriched
    .select(
        "id",
        "account_id",
        "amount",
        "txn_type_lower",
        "prev_txn_time",
        "session_start_flag",
        "new_group_flag",
        "first_known_session_start_flag",
        "account_opened_date",
    )
    .orderBy(F.col("id").desc_nulls_last())
)
