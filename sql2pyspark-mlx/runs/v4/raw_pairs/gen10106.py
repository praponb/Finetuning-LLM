from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc_nulls_last(), "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w3 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("merchant").orderBy("is_flagged_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("prev_txn_time", "id")
w6 = Window.partitionBy("merchant", "txn_type").orderBy("txn_time", "id")
w7 = Window.partitionBy("merchant").orderBy(F.col("prev_txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w_ordered = Window.partitionBy("account_id", "merchant").orderBy("prev_txn_time_2", "id")
w9 = Window.partitionBy("merchant").orderBy(F.col("prev_txn_time_2").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w_hist = Window.partitionBy("account_id", "merchant").orderBy("is_new_burst_2_count_90d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("account_id")
w_recent = Window.orderBy(F.col("is_flagged_flag").desc(), "id")
w13 = Window.partitionBy("island_id").orderBy(F.col("distinct_ids").desc())

cleaned = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2024-02-23").cast("date")) & F.col("merchant").isNotNull())
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
)

ranked = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

prev_events = ranked.withColumn("prev_txn_time", F.lag("txn_time").over(w2))

flagged = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("burst_id", F.sum("is_new_burst").over(w3))

with_prev = sessions_transactions.withColumn("prev_txn_time_2", F.lag("txn_time").over(w2))

gaps = (
    with_prev
    .withColumn("is_new_burst_2", F.when(F.col("prev_txn_time_2").isNull() | (F.col("txn_time") > F.col("prev_txn_time_2") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("is_new_burst_2").over(w3))

scored = (
    sessionized
    .withColumn("first_known_amount", F.first("amount", ignorenulls=True).over(w4))
    .withColumn("seq_no", F.row_number().over(w5))
    .withColumn("next_is_new_burst_2", F.lead("is_new_burst_2", 1).over(w6))
    .withColumn("is_new_burst_2_count_90d", F.count("is_new_burst_2").over(w7))
    .withColumn("prev_burst_id", F.lag("burst_id", 1).over(w_ordered))
)

scored_transactions = (
    scored
    .select(
        "*",
        F.avg("amount").over(w9).alias("amount_avg_30d"),
        F.first("amount", ignorenulls=True).over(w_hist).alias("first_known_amount_2"),
        F.min("is_new_burst_2_count_90d").over(w11).alias("account_id_min_is_new_burst_2_count_90d"),
        F.row_number().over(w_recent).alias("row_seq"),
    )
)

result = (
    scored_transactions
    .groupBy("island_id", F.date_trunc("year", "prev_txn_time_2").alias("year_start"))
    .agg(
        F.sum(F.when(F.col("merchant") == "Amazon", F.col("is_new_burst")).otherwise(0)).alias("amazon_is_new_burst"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("merchant") == "Amazon", 1)).alias("amazon_rows"),
    )
    .withColumn("distinct_ids_rank", F.rank().over(w13))
)
