from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy("prev_txn_time", "id")
w4 = Window.partitionBy("txn_type").orderBy("prev_txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter(F.col("txn_type").isin(["DEBIT", "FEE", "CREDIT"]))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0).alias("is_credit"),
        F.coalesce("merchant", F.lit("NONE")).alias("resolved_merchant"),
    )
)

prev_events = valid.withColumn("prev_txn_time", F.lag("txn_time").over(w))

flagged = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("new_group_flag").over(w2))

flagged_2 = (
    islands
    .withColumn("session_start_flag", F.when(F.lag("prev_txn_time").over(w3).isNull() | (F.col("prev_txn_time") > F.lag("prev_txn_time").over(w3) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_transactions = (
    flagged_2
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "is_flagged",
        "resolved_merchant",
        "prev_txn_time",
        "burst_id",
        F.sum("session_start_flag").over(w4).alias("island_id"),
    )
)

valid_transactions = (
    sessions_transactions
    .filter(F.col("prev_txn_time") >= "2023-10-18")
    .withColumn("burst_id_rounded", F.round("burst_id", 2))
)

valid_transactions_2 = (
    valid_transactions
    .filter(F.col("burst_id") > 3)
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "resolved_merchant",
        "prev_txn_time",
        F.round("burst_id", 0).alias("burst_id_rounded_2"),
    )
)

joined = (
    valid_transactions_2
    .alias("s")
    .join(df_accounts.alias("d"), F.col("s.account_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.account_type"), F.col("d.opened_date"))
)

result = (
    joined
    .groupBy(
        F.when(F.col("burst_id_rounded_2") >= 68, "PREMIUM").otherwise("STANDARD").alias("burst_id_rounded_2_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.count("*").alias("event_count_2"),
        F.round(F.sum("burst_id_rounded_2") / F.nullif(F.count(F.when(F.col("txn_type") == "REFUND", 1)), F.lit(0)), 2).alias("burst_id_rounded_2_per_refund"),
        F.count("*").alias("num_rows"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.sum(F.when(F.col("account_type") == "CHECKING", F.col("burst_id_rounded_2")).otherwise(0)).alias("checking_burst_id_rounded_2"),
    )
    .orderBy("checking_burst_id_rounded_2")
)
