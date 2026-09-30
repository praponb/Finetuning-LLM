from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("merchant", "account_id").orderBy("is_new_session", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w5 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter(F.col("account_id").isin([12, 21]))
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        F.hour("txn_time").alias("txn_time_hour"),
    )
)

with_gaps = (
    base
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

metrics = (
    sessions_transactions
    .select(
        "*",
        F.sum("is_new_session").over(w3).alias("running_sum_is_new_session"),
        F.count("is_new_session").over(w3).alias("running_count_is_new_session"),
        F.max("is_new_session").over(w3).alias("running_max_is_new_session"),
    )
)

flagged = (
    metrics
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w4) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_transactions_2 = flagged.withColumn("session_no_2", F.sum("session_start_flag").over(w5))

cleaned = (
    sessions_transactions_2
    .filter((~F.col("merchant").eqNullSafe(F.lit("Amazon"))) & F.col("merchant").isNotNull())
    .select(
        "*",
        (F.col("txn_time_hour") + F.col("is_new_session")).alias("txn_time_hour_plus_is_new_session"),
        F.length("txn_type").alias("txn_type_length"),
        F.coalesce("merchant", F.lit("OTHER")).alias("resolved_merchant"),
    )
)

filtered = (
    cleaned
    .filter(F.col("merchant").isNotNull() & (~F.col("merchant").eqNullSafe(F.lit("7-Eleven"))))
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
)

pivoted = (
    filtered
    .groupBy("merchant")
    .pivot("txn_type", ["DEBIT", "REFUND"])
    .agg(F.avg("running_max_is_new_session").alias("value_sum"))
    .select("merchant", F.col("DEBIT").alias("debit"), F.col("REFUND").alias("refund"))
)

result = (
    pivoted
    .select("merchant", "refund")
    .orderBy(F.col("merchant").desc_nulls_last(), "refund")
)
