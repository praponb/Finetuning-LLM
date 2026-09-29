from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id", "merchant").orderBy("amount", "id")
w4 = Window.partitionBy("account_id", "txn_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w6 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("merchant", "txn_type").orderBy(F.col("txn_time").desc(), "id")
w_grp = Window.orderBy("is_new_session_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("account_id")
w_ordered = Window.orderBy("txn_time", "id")

enriched = (
    df_transactions
    .alias("e")
    .join(df_accounts.alias("d"), F.col("e.account_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.opened_date"), F.col("d.balance"))
)

flagged = (
    enriched
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w2))

scored = (
    sessionized
    .select(
        "*",
        F.ntile(4).over(w3).alias("amount_bucket"),
        F.last("balance", ignorenulls=True).over(w4).alias("last_known_balance"),
    )
)

with_gaps = (
    scored
    .withColumn("is_new_session_2", F.when(F.lag("txn_time").over(w5).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w5) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("is_new_session_2").over(w6))

enriched_transactions = (
    islands
    .select(
        "*",
        F.row_number().over(w7).alias("event_seq"),
        F.last("balance", ignorenulls=True).over(w_grp).alias("last_known_balance_2"),
    )
)

metrics = (
    enriched_transactions
    .select(
        "*",
        F.round((F.col("event_seq") / F.sum("event_seq").over(w9)), 4).alias("event_seq_share"),
        F.lag("is_new_session", 2).over(w_ordered).alias("prev_is_new_session"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("is_new_session_2") >= 1, "HIGH").otherwise("LOW").alias("is_new_session_2_tier"),
        F.when(F.col("prev_is_new_session") >= 1, "HIGH").otherwise("LOW").alias("prev_is_new_session_tier"),
    )
    .agg(
        F.count(F.when(F.col("merchant") == "Starbucks", 1)).alias("starbucks_count"),
        F.percentile_approx("amount_bucket", 0.95).alias("amount_bucket_p95"),
        F.min("island_id").alias("min_island_id"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
    )
)
