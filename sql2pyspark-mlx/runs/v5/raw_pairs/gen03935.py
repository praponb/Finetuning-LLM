from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("txn_type").orderBy(F.col("txn_time").desc(), "id")
w3 = Window.partitionBy("account_id").orderBy("is_flagged_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w5 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter(F.col("txn_time") > F.lit("2023-07-25 15:00:00").cast("timestamp"))
    .select("*", F.col("is_flagged").cast("int").alias("is_flagged_flag"))
)

with_windows = (
    valid
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("row_seq"),
        F.sum("amount").over(w3).alias("cumulative_amount"),
    )
)

with_gaps = (
    with_windows
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w4).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w4) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w5))

filtered = (
    sessions_transactions
    .filter(~F.col("merchant").eqNullSafe(F.lit("7-Eleven")))
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged_flag",
        "event_seq",
        "cumulative_amount",
        "is_new_burst",
        "burst_id",
        F.ceil("event_seq").alias("event_seq_whole"),
    )
)

with_dims = (
    filtered
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "left")
    .select(
        "s.id",
        "s.account_id",
        "s.txn_type",
        "s.txn_time",
        "s.merchant",
        "s.is_flagged_flag",
        "s.event_seq",
        F.col("a.status"),
    )
)

result = (
    with_dims
    .rollup(F.date_trunc("YEAR", "txn_time").alias("year_start"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.min("txn_time").alias("started_at"),
        F.sum(F.when(F.col("txn_type") == "REFUND", F.col("event_seq")).otherwise(0)).alias("refund_event_seq"),
        F.countDistinct("merchant").alias("distinct_merchants"),
        F.percentile_approx("event_seq", 0.9).alias("event_seq_p90"),
    )
)
