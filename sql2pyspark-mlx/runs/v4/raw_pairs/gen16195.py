from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-07-25 16:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("resolved_merchant"),
    )
)

with_prev = base.withColumn("prev_txn_time", F.lag("txn_time").over(w))

gaps = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_transactions = gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_transactions
    .withColumn("session_start_flag_tier", F.when(F.col("session_start_flag") >= 1, "HIGH").otherwise("LOW"))
    .cube("session_start_flag_tier", "account_id")
    .agg(
        F.sum(F.when(F.col("merchant") == "Amazon", F.col("island_id"))).alias("amazon_total_island_id"),
        F.countDistinct("account_id").alias("distinct_account_ids"),
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.percentile_approx("island_id", 0.95).alias("island_id_p95"),
        F.grouping("session_start_flag_tier").alias("gid"),
    )
    .orderBy(F.col("session_start_flag_tier").asc_nulls_last())
)
