from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec_filled").desc(), "id")
w2 = Window.orderBy(F.col("duration_sec").asc_nulls_first())
w3 = Window.partitionBy("page").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("page").orderBy("duration_sec", "id")
w5 = Window.partitionBy("page").orderBy("event_time", "id")
w6 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("week_start").orderBy(F.col("search_events").desc())

filtered = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-04-11").cast("date")) & F.col("page").isNotNull())
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
    )
)

enriched = (
    filtered
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        F.row_number().over(w).alias("seq_no"),
        F.cume_dist().over(w2).alias("duration_sec_cume_dist"),
    )
)

valid = (
    enriched
    .filter((F.col("event_time") >= F.lit("2023-03-27").cast("date")) & (F.col("duration_sec") < 238))
    .select(
        "*",
        F.date_format("event_time", "yyyy-MM").alias("event_time_label"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("resolved_page"),
    )
)

enriched_web_events = (
    valid
    .select(
        "*",
        F.sum("duration_sec").over(w3).alias("running_sum_duration_sec"),
        F.count("duration_sec").over(w3).alias("running_count_duration_sec"),
        F.avg("duration_sec").over(w3).alias("running_avg_duration_sec"),
        F.min("duration_sec").over(w3).alias("running_min_duration_sec"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

filtered_web_events = (
    enriched_web_events
    .filter(F.col("running_avg_duration_sec") <= 400)
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        "duration_sec_cume_dist",
        "resolved_page",
        "running_count_duration_sec",
        "running_avg_duration_sec",
        "running_min_duration_sec",
        "row_seq",
        (F.col("duration_sec") - F.col("running_sum_duration_sec")).alias("duration_sec_minus_running_sum_duration_sec"),
        F.length("page").alias("page_length"),
    )
)

with_prev = filtered_web_events.withColumn("prev_event_time", F.lag("event_time").over(w5))

with_gaps = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w6))

prev_events = sessionized.withColumn("prev_event_time_2", F.lag("event_time").over(w5))

gaps = (
    prev_events
    .withColumn("new_group_flag_2", F.when(F.col("prev_event_time_2").isNull() | (F.col("event_time") > F.col("prev_event_time_2") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "page",
        "event_time",
        "running_avg_duration_sec",
        "running_min_duration_sec",
        "duration_sec_minus_running_sum_duration_sec",
        "page_length",
        "prev_event_time",
        "new_group_flag",
        F.sum("new_group_flag_2").over(w6).alias("island_id"),
    )
)

result = (
    islands
    .groupBy(
        F.date_trunc("WEEK", "event_time").alias("week_start"),
        F.date_trunc("MONTH", "event_time").alias("activity_month"),
    )
    .agg(
        F.max("new_group_flag").alias("max_new_group_flag"),
        F.count_if(F.col("page") == "/search").alias("search_events"),
        F.round(F.avg("island_id"), 2).alias("avg_island_id"),
    )
    .withColumn("search_events_percent_rank", F.percent_rank().over(w7))
    .orderBy(F.col("avg_island_id").asc_nulls_first())
)
