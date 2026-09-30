from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("txn_type")
w5 = Window.partitionBy("txn_type").orderBy("session_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("txn_type").orderBy(F.col("cumulative_session_id").desc(), "id")
w7 = Window.partitionBy("txn_type").orderBy("is_new_burst", "id")
w8 = Window.partitionBy("txn_type").orderBy(F.col("is_new_burst").desc(), "id")

filtered = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-10-15 20:00:00").cast("timestamp")) & F.col("merchant").isNotNull() & F.col("is_flagged"))
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
)

flagged = (
    filtered
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_burst").over(w2))

with_windows = (
    sessionized
    .select(
        "*",
        F.sum("session_id").over(w3).alias("cumulative_session_id"),
        F.stddev_samp("is_flagged_flag").over(w4).alias("txn_type_stddev_samp_is_flagged_flag"),
        F.last("is_flagged_flag", ignorenulls=True).over(w5).alias("last_known_is_flagged_flag"),
    )
)

valid = (
    with_windows
    .filter(F.col("txn_type_stddev_samp_is_flagged_flag") >= 39)
    .select(
        "id",
        "txn_type",
        "amount",
        "is_flagged_flag",
        "is_new_burst",
        "session_id",
        "cumulative_session_id",
        "txn_type_stddev_samp_is_flagged_flag",
        F.substring("txn_type", 1, 2).alias("txn_type_prefix"),
        F.datediff(F.current_date(), "txn_time").alias("days_since_txn_time"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.row_number().over(w6).alias("row_seq"),
        F.row_number().over(w7).alias("seq_no"),
        F.ntile(3).over(w8).alias("is_new_burst_bucket"),
    )
)

cleaned = (
    scored
    .filter(F.col("is_new_burst_bucket") != 4)
    .withColumn("txn_type_stddev_samp_is_flagged_flag_scaled", F.round((F.col("txn_type_stddev_samp_is_flagged_flag") * 1.15), 2))
    .withColumn("clean_txn_type", F.trim("txn_type"))
)

stats = (
    cleaned
    .groupBy(
        F.when(F.col("session_id") >= 6, "LARGE").otherwise("SMALL").alias("session_id_tier"),
        "seq_no",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("cumulative_session_id", [0.25, 0.5, 0.75]).alias("cumulative_session_id_quartiles"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
        F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_count"),
        F.percentile_approx("txn_type_stddev_samp_is_flagged_flag_scaled", 0.95).alias("txn_type_stddev_samp_is_flagged_flag_scaled_p95"),
    )
    .filter(F.col("avg_seq_no") >= 0)
)

result = (
    stats
    .select(
        "session_id_tier",
        "seq_no",
        "credit_count",
        "txn_type_stddev_samp_is_flagged_flag_scaled_p95",
    )
    .orderBy(
        F.col("session_id_tier").asc_nulls_last(),
        "txn_type_stddev_samp_is_flagged_flag_scaled_p95",
    )
)
