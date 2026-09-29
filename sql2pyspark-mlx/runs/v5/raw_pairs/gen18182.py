from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("txn_time").desc_nulls_first(), "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id")
w3 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_id", "merchant").orderBy("burst_id", "id")
w5 = Window.partitionBy("account_id").orderBy(F.col("txn_time").asc_nulls_last(), "id")
w6 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w7 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.max("amount").alias("max_transaction_amount"))

latest = (
    df_transactions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "account_id", "amount", "txn_time", "merchant")
)

with_gaps = (
    latest
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w2).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w2) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("burst_id", F.sum("new_group_flag").over(w3))

with_windows = (
    sessions_transactions
    .select(
        "*",
        F.lead("new_group_flag").over(w4).alias("next_new_group_flag"),
        F.lag("new_group_flag").over(w4).alias("prev_new_group_flag"),
        F.lead("amount").over(w4).alias("next_amount"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

latest_transactions = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

prev_events = latest_transactions.withColumn("prev_txn_time", F.lag("txn_time").over(w6))

gaps = (
    prev_events
    .withColumn("new_group_flag_2", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_transactions_2 = gaps.withColumn("session_no", F.sum("new_group_flag_2").over(w7))

result = (
    sessions_transactions_2
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "amount",
        "merchant",
        "new_group_flag",
        "burst_id",
        "next_new_group_flag",
        "next_amount",
        "event_seq",
        F.when(F.col("merchant").rlike("^(Grab|Amazon)$"), "OTHER_ZONE").when(F.col("merchant").rlike("^(7-Eleven|Lazada)$"), "PRIORITY").otherwise("EXTENDED").alias("merchant_zone"),
        F.col("max_transaction_amount"),
    )
    .orderBy(F.col("next_new_group_flag").asc_nulls_last())
)
