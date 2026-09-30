from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w2 = Window.orderBy(F.col("txn_time").desc(), "id")
w3 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w4 = Window.partitionBy("txn_type", "merchant").orderBy(F.col("amount").desc())
w5 = Window.partitionBy("txn_type").orderBy(F.col("txn_time_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w6 = Window.partitionBy("account_id").orderBy("txn_time", "id")
w7 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("txn_type", "merchant").orderBy("amount_count_7d")
w9 = Window.partitionBy("txn_type")
w10 = Window.partitionBy("txn_type").orderBy(F.col("amount_avg_30d").asc_nulls_first())
w11 = Window.partitionBy("merchant").orderBy("burst_id", "id")
w12 = Window.partitionBy("merchant", "account_id")
w13 = Window.orderBy("is_new_session", "id").rowsBetween(-3, Window.currentRow)
w14 = Window.orderBy(F.col("amount_percent_rank").desc(), "id")
w15 = Window.orderBy("event_seq", "id").rowsBetween(-3, -1)

valid = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2024-05-10").cast("date")) & F.col("merchant").isNotNull())
    .select("*", F.lower("txn_type").alias("txn_type_lower"))
)

filtered = (
    valid
    .filter(F.col("txn_time") >= F.lit("2023-12-24 21:00:00").cast("timestamp"))
    .select(
        "*",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.date_trunc("WEEK", "txn_time").alias("txn_time_period"),
    )
)

enriched = (
    filtered
    .withColumn("amount_avg_30d", F.avg("amount").over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("amount_percent_rank", F.percent_rank().over(w4))
    .withColumn("amount_count_7d", F.count("amount").over(w5))
)

prev_events = enriched.withColumn("prev_txn_time", F.lag("txn_time").over(w6))

with_gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("is_new_session").over(w7))

ranked = (
    sessionized
    .withColumn("rank_in_group", F.rank().over(w8))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_windows = (
    ranked
    .withColumn("amount_avg_30d_share", F.round((F.col("amount_avg_30d") / F.sum("amount_avg_30d").over(w9)), 4))
    .withColumn("amount_avg_30d_rank", F.rank().over(w10))
    .withColumn("prev_seq_no", F.lag("seq_no", 1).over(w11))
    .withColumn("merchant_stddev_samp_is_new_session", F.stddev_samp("is_new_session").over(w12))
)

enriched_transactions = (
    with_windows
    .select(
        "*",
        F.avg("event_seq").over(w13).alias("event_seq_avg_3"),
        F.row_number().over(w14).alias("seq_no_2"),
        F.avg("amount_percent_rank").over(w15).alias("amount_percent_rank_avg_3_prior"),
    )
)

result = (
    enriched_transactions
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "txn_type_lower",
        "is_flagged_label",
        "txn_time_period",
        "amount_avg_30d",
    )
    .orderBy("txn_time_period")
)
