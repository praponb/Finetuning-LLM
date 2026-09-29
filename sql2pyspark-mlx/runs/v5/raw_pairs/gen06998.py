from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("duration_sec_filled").asc_nulls_last())
w4 = Window.partitionBy("page")
w_ordered = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(-5, Window.currentRow)
w6 = Window.partitionBy("event_type").orderBy("island_id", "id").rowsBetween(-6, Window.currentRow)
w7 = Window.orderBy("last7_max_duration_sec_filled_dense_rank", "id")
w8 = Window.orderBy(F.col("last7_count_duration_sec_filled_dense_rank").desc(), "id")
w9 = Window.orderBy(F.col("duration_sec_avg_5").asc_nulls_first())
w10 = Window.orderBy(F.col("event_seq_2").desc_nulls_first())
w11 = Window.orderBy("event_seq_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter((F.col("event_time") >= F.to_date(F.lit("2023-06-14"))) & F.col("page").isNotNull())
    .select(
        "*",
        F.date_format("event_time", "MMM yyyy").alias("event_time_label"),
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
    )
)

gaps = (
    base
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

metrics = (
    sessionized
    .select(
        "*",
        F.dense_rank().over(w3).alias("duration_sec_filled_dense_rank"),
        F.stddev_samp("island_id").over(w4).alias("page_stddev_samp_island_id"),
        F.avg("duration_sec").over(w_ordered).alias("duration_sec_avg_5"),
    )
)

metrics_web_events = (
    metrics
    .select(
        "id",
        "event_time",
        "event_time_label",
        "is_new_session",
        "duration_sec_filled_dense_rank",
        "duration_sec_avg_5",
        F.sum("duration_sec_filled_dense_rank").over(w6).alias("last7_sum_duration_sec_filled_dense_rank"),
        F.count("duration_sec_filled_dense_rank").over(w6).alias("last7_count_duration_sec_filled_dense_rank"),
        F.min("duration_sec_filled_dense_rank").over(w6).alias("last7_min_duration_sec_filled_dense_rank"),
        F.max("duration_sec_filled_dense_rank").over(w6).alias("last7_max_duration_sec_filled_dense_rank"),
    )
)

scored = (
    metrics_web_events
    .select(
        "id",
        "event_time_label",
        "is_new_session",
        "last7_sum_duration_sec_filled_dense_rank",
        "last7_count_duration_sec_filled_dense_rank",
        F.row_number().over(w7).alias("event_seq"),
        F.row_number().over(w8).alias("event_seq_2"),
        F.dense_rank().over(w9).alias("duration_sec_avg_5_dense_rank"),
    )
)

result = (
    scored
    .filter(F.col("last7_sum_duration_sec_filled_dense_rank") >= 27)
    .select(
        "id",
        "event_time_label",
        "last7_count_duration_sec_filled_dense_rank",
        "event_seq",
        "duration_sec_avg_5_dense_rank",
        F.dense_rank().over(w10).alias("event_seq_2_dense_rank"),
        F.last("event_seq", ignorenulls=True).over(w11).alias("last_known_event_seq"),
    )
)
