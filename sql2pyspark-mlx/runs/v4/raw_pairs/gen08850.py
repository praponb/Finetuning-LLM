from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w4 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_transactions
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("island_id", F.sum("session_start_flag").over(w2))

with_prev = sessions_transactions.withColumn("prev_txn_time", F.lag("txn_time").over(w3))

with_gaps = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_transactions_2 = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w4))

joined = (
    sessions_transactions_2
    .alias("e")
    .join(df_accounts.alias("p"), F.col("e.account_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.opened_date"))
)

prev_events = joined.withColumn("prev_txn_time_2", F.lag("txn_time").over(w))

flagged_2 = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_txn_time_2").isNull() | (F.col("txn_time") > F.col("prev_txn_time_2") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = flagged_2.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .cube(
        F.when(F.col("island_id") >= 6, "HIGH").otherwise("LOW").alias("island_id_tier"),
    )
    .agg(
        F.countDistinct("account_id").alias("distinct_account_ids"),
        F.sum(F.when(F.col("merchant") == "Starbucks", F.col("amount"))).alias("starbucks_total_amount"),
        F.count(F.when(F.col("merchant") == "Shell", 1)).alias("shell_rows"),
    )
    .orderBy("island_id_tier")
)
