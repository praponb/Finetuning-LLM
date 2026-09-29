from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("merchant", "account_id").orderBy("txn_time", "id")
w_ordered = Window.partitionBy("account_id").orderBy("txn_time", "id")
w4 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_time",
        "merchant",
        "is_flagged",
        F.date_format("txn_time", "yyyy-MM-dd").alias("txn_time_label"),
        F.substring("txn_type", 1, 3).alias("txn_type_prefix"),
    )
)

enriched = (
    filtered
    .withColumn("first_known_merchant", F.first("merchant", ignorenulls=True).over(w_recent))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("event_seq", F.row_number().over(w_ordered))
)

with_windows = (
    enriched
    .withColumn("running_seq_no", F.sum("seq_no").over(w4))
    .withColumn("prev_event_seq", F.lag("event_seq", 1).over(w_ordered))
)

cleaned = (
    with_windows
    .filter((F.col("txn_time") > F.to_date(F.lit("2023-05-09"))) & F.col("merchant").isNotNull())
    .select(
        "*",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
)

result = (
    cleaned
    .groupBy("merchant")
    .agg(
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.min("txn_time").alias("first_seen"),
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
    )
)
