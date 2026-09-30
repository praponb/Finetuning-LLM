from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec_rounded").desc(), "id")
w2 = Window.partitionBy("page").orderBy("duration_sec_rounded")
w_recent = Window.partitionBy("page")
w4 = Window.partitionBy("session_id").orderBy("event_time", "id")
w5 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("page").orderBy("event_time", "id")
w7 = Window.partitionBy("page").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("page").orderBy(F.col("session_no").desc(), "id")

cleaned = (
    df_web_events
    .filter(F.col("event_time") > F.lit("2023-10-17 10:00:00").cast("timestamp"))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.round("duration_sec", 2).alias("duration_sec_rounded"),
    )
)

latest = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    latest
    .select(
        "*",
        F.rank().over(w2).alias("duration_sec_rounded_rank"),
        F.count("duration_sec").over(w_recent).alias("page_count_duration_sec"),
    )
)

with_gaps = (
    metrics
    .withColumn("new_group_flag", F.when(F.col("event_time") - F.lag("event_time").over(w4) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec_rounded",
        "rank_in_group",
        F.sum("new_group_flag").over(w5).alias("session_no"),
    )
)

cleaned_web_events = (
    sessionized
    .filter((~F.col("page").eqNullSafe(F.lit("/cart"))) & (F.col("event_time") >= F.lit("2023-09-27").cast("date")))
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec_rounded",
        "rank_in_group",
        "session_no",
        F.upper("page").alias("page_upper"),
        F.date_format("event_time", "yyyy-MM").alias("event_time_label"),
    )
)

metrics_web_events = (
    cleaned_web_events
    .select(
        "id",
        "page",
        "session_no",
        F.lead("duration_sec_rounded").over(w6).alias("next_duration_sec_rounded"),
        F.lead("session_no").over(w6).alias("next_session_no"),
        F.lag("session_no").over(w6).alias("prev_session_no"),
        F.row_number().over(w6).alias("event_seq"),
        F.sum("duration_sec_rounded").over(w7).alias("cumulative_duration_sec_rounded"),
    )
)

deduped = (
    metrics_web_events
    .withColumn("rank_in_group", F.row_number().over(w8))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .groupBy(
        F.when(F.col("next_duration_sec_rounded") >= 80, "PREMIUM").otherwise("STANDARD").alias("next_duration_sec_rounded_tier"),
        "session_no",
    )
    .agg(
        F.percentile_approx("session_no", 0.95).alias("session_no_p95"),
        F.round(F.avg("next_session_no"), 2).alias("avg_next_session_no"),
        F.round(F.avg("next_session_no"), 2).alias("avg_next_session_no_2"),
    )
    .orderBy(F.col("session_no").desc())
)
