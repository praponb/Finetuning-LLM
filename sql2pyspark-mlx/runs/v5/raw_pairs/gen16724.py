from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(-5, Window.currentRow)
w_hist = Window.partitionBy("merchant").orderBy("amount_avg_5", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("merchant").orderBy(F.col("island_id").desc())

prev_events = df_transactions.withColumn("prev_txn_time", F.lag("txn_time").over(w))

gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_transactions = (
    gaps
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        F.sum("session_start_flag").over(w2).alias("island_id"),
    )
)

metrics = (
    sessions_transactions
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        "island_id",
        F.avg("amount").over(w3).alias("amount_avg_5"),
        F.first("merchant", ignorenulls=True).over(w2).alias("first_known_merchant"),
    )
)

base = (
    metrics
    .filter(F.col("txn_time") > F.lit("2023-06-08").cast("date"))
    .withColumn("least_island_id_amount", F.least("island_id", "amount"))
)

filtered = (
    base
    .filter(F.col("merchant").isNotNull() & (F.col("island_id") > 5))
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        "island_id",
        "amount_avg_5",
        "first_known_merchant",
        "least_island_id_amount",
        F.date_format("txn_time", "yyyy-MM").alias("txn_time_label"),
    )
)

scored = (
    filtered
    .filter(F.col("txn_time") >= "2025-07-01")
    .select(
        "*",
        F.sum("island_id").over(w_hist).alias("last3_sum_island_id"),
        F.count("island_id").over(w_hist).alias("last3_count_island_id"),
        F.avg("island_id").over(w_hist).alias("last3_avg_island_id"),
        F.rank().over(w5).alias("island_id_rank"),
    )
)

flagged = (
    scored
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .groupBy(F.date_trunc("MONTH", "txn_time").alias("activity_month"), "session_id")
    .agg(
        F.sum(F.when(F.col("merchant") == "Grab", F.col("new_group_flag")).otherwise(0)).alias("grab_new_group_flag"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
    )
    .orderBy("grab_new_group_flag")
)
