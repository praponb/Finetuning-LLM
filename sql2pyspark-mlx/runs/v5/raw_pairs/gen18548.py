from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "merchant").orderBy(F.col("is_flagged_flag").desc_nulls_first(), "id")
w2 = Window.partitionBy("txn_type").orderBy("is_flagged_flag", "id")
w3 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w4 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("txn_type", "merchant").orderBy("seq_no", "id")
w6 = Window.partitionBy("merchant").orderBy(F.col("seq_no").desc_nulls_last())
w7 = Window.partitionBy("merchant")
w8 = Window.orderBy(F.col("seq_no").desc(), "id")
w9 = Window.orderBy("is_flagged_flag", "id")
w10 = Window.orderBy("prev_prev_is_flagged_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("merchant").orderBy("merchant_count_session_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("merchant", "txn_type").orderBy("seq_no_bucket", "id")

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
    .withColumn("resolved_merchant", F.coalesce("merchant", F.lit("NONE")))
)

latest = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    latest
    .withColumn("next_amount", F.lead("amount").over(w2))
    .withColumn("prev_is_flagged_flag", F.lag("is_flagged_flag").over(w2))
    .withColumn("next_is_flagged_flag", F.lead("is_flagged_flag").over(w2))
    .withColumn("seq_no", F.row_number().over(w2))
)

flagged = (
    scored
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w3) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged_flag",
        "resolved_merchant",
        "next_amount",
        "prev_is_flagged_flag",
        "seq_no",
        F.sum("session_start_flag").over(w4).alias("session_id"),
    )
)

with_windows = (
    islands
    .select(
        "id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged_flag",
        "next_amount",
        "prev_is_flagged_flag",
        "seq_no",
        F.lag("prev_is_flagged_flag", 1).over(w5).alias("prev_prev_is_flagged_flag"),
        F.percent_rank().over(w6).alias("seq_no_percent_rank"),
        F.count("session_id").over(w7).alias("merchant_count_session_id"),
        F.ntile(3).over(w8).alias("seq_no_bucket"),
        F.lag("seq_no", 1).over(w9).alias("prev_seq_no"),
    )
)

with_windows_transactions = (
    with_windows
    .filter(F.col("next_amount") < 2900)
    .select(
        "id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged_flag",
        "next_amount",
        "seq_no",
        "prev_prev_is_flagged_flag",
        "seq_no_percent_rank",
        "merchant_count_session_id",
        "seq_no_bucket",
        F.last("prev_seq_no", ignorenulls=True).over(w10).alias("last_known_prev_seq_no"),
        F.sum("prev_seq_no").over(w11).alias("running_prev_seq_no"),
    )
)

deduped = (
    with_windows_transactions
    .withColumn("latest_rank", F.row_number().over(w12))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .cube("last_known_prev_seq_no")
    .agg(
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        F.percentile_approx("next_amount", 0.95).alias("next_amount_p95"),
        F.max("txn_time").alias("last_txn_time"),
        F.percentile_approx("next_amount", 0.9).alias("next_amount_p90"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
    )
    .orderBy("duration_min")
)
