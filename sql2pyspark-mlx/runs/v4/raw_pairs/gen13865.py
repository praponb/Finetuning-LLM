from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("duration_sec_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("duration_sec_adjusted", "id")
w5 = Window.orderBy("first_known_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("duration_sec_adjusted_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("duration_sec_adjusted_bucket").desc_nulls_last())
w8 = Window.orderBy("seq_no", "id")
w_recent = Window.orderBy(F.col("running_first_known_duration_sec").desc(), "id")
w10 = Window.orderBy(F.col("duration_sec_adjusted_bucket_rank").desc_nulls_last())
w11 = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.orderBy("running_first_known_duration_sec_2", "id").rowsBetween(-2, Window.currentRow)

flagged = (
    df_web_events
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("session_id_2", F.sum("session_start_flag").over(w2))

filtered = (
    islands
    .filter(F.col("event_time") > F.lit("2024-03-20").cast("date"))
    .select(
        "id",
        "duration_sec",
        F.date_format("event_time", "yyyy-MM").alias("event_time_label"),
    )
)

filtered_web_events = (
    filtered
    .filter(F.col("duration_sec").between(194, 222))
    .withColumn("duration_sec_band", F.when(F.col("duration_sec") >= 376, "High").when(F.col("duration_sec") >= 125, "Medium").otherwise("Low"))
    .withColumn("duration_sec_adjusted", F.round((F.col("duration_sec") * 100), 2))
)

metrics = (
    filtered_web_events
    .withColumn("first_known_duration_sec", F.first("duration_sec", ignorenulls=True).over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
    .withColumn("duration_sec_adjusted_bucket", F.ntile(4).over(w4))
)

scored = (
    metrics
    .withColumn("cumulative_first_known_duration_sec", F.sum("first_known_duration_sec").over(w5))
    .withColumn("prev_seq_no", F.lag("seq_no", 1).over(w4))
    .withColumn("running_first_known_duration_sec", F.sum("first_known_duration_sec").over(w6))
    .withColumn("duration_sec_adjusted_bucket_rank", F.rank().over(w7))
    .withColumn("running_first_known_duration_sec_2", F.sum("first_known_duration_sec").over(w5))
)

scored_web_events = (
    scored
    .select(
        "*",
        F.ntile(4).over(w8).alias("seq_no_bucket"),
        F.row_number().over(w_recent).alias("row_seq"),
        F.dense_rank().over(w10).alias("duration_sec_adjusted_bucket_rank_dense_rank"),
    )
)

with_windows = (
    scored_web_events
    .select(
        "*",
        F.sum("running_first_known_duration_sec_2").over(w11).alias("cumulative_running_first_known_duration_sec_2"),
        F.last("running_first_known_duration_sec_2", ignorenulls=True).over(w11).alias("last_known_running_first_known_duration_sec_2"),
        F.avg("seq_no_bucket").over(w12).alias("seq_no_bucket_avg_2"),
    )
)

grouped = (
    with_windows
    .groupBy(
        F.when(F.col("prev_seq_no") >= 7, "LARGE").otherwise("SMALL").alias("prev_seq_no_tier"),
        F.when(F.col("running_first_known_duration_sec_2") >= 2273, "LARGE").otherwise("SMALL").alias("running_first_known_duration_sec_2_tier"),
    )
    .agg(
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.min("running_first_known_duration_sec_2").alias("min_running_first_known_duration_sec_2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("seq_no_bucket", 0.9).alias("seq_no_bucket_p90"),
    )
)

result = (
    grouped
    .select(
        "prev_seq_no_tier",
        "running_first_known_duration_sec_2_tier",
        "duration_sec_bit_and",
        "distinct_ids",
    )
    .orderBy(F.col("distinct_ids").desc(), "running_first_known_duration_sec_2_tier")
)
