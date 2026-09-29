from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page").orderBy("days_since_event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("cumulative_session_id_2", "id")
w5 = Window.orderBy("session_id_2", "id")
w6 = Window.orderBy(F.col("cumulative_session_id_2_rounded").desc(), "id")
w7 = Window.orderBy(F.col("cumulative_session_id_2_rounded").asc_nulls_first())
w8 = Window.orderBy("event_seq", "id")
w9 = Window.orderBy("cumulative_session_id_2_rounded_bucket", "id")

gaps = (
    df_web_events
    .withColumn("new_group_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_web_events = (
    gaps
    .select(
        "id",
        "page",
        "event_time",
        F.sum("new_group_flag").over(w2).alias("session_id_2"),
    )
)

valid = (
    sessions_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "*",
        F.datediff(F.current_date(), "event_time").alias("days_since_event_time"),
    )
)

enriched = (
    valid
    .select(
        "id",
        "session_id_2",
        F.lead("days_since_event_time", 2).over(w).alias("next_days_since_event_time"),
        F.sum("session_id_2").over(w3).alias("cumulative_session_id_2"),
    )
)

valid_web_events = (
    enriched
    .filter(F.col("cumulative_session_id_2").isNotNull())
    .select(
        "*",
        F.round("cumulative_session_id_2", 0).alias("cumulative_session_id_2_rounded"),
    )
)

metrics = (
    valid_web_events
    .select(
        "id",
        "session_id_2",
        F.row_number().over(w4).alias("row_seq"),
        F.row_number().over(w5).alias("event_seq"),
        F.ntile(5).over(w6).alias("cumulative_session_id_2_rounded_bucket"),
        F.dense_rank().over(w7).alias("cumulative_session_id_2_rounded_dense_rank"),
    )
)

scored = (
    metrics
    .filter(F.col("cumulative_session_id_2_rounded_bucket") == 4)
    .withColumn("seq_no", F.row_number().over(w8))
    .withColumn("next_cumulative_session_id_2_rounded_dense_rank", F.lead("cumulative_session_id_2_rounded_dense_rank", 1).over(w9))
)

report = (
    scored
    .select(
        "id",
        "row_seq",
        "event_seq",
        "cumulative_session_id_2_rounded_bucket",
        "seq_no",
    )
)

result = (
    report
    .select(
        "id",
        "row_seq",
        "event_seq",
        "cumulative_session_id_2_rounded_bucket",
        "seq_no",
    )
    .orderBy(F.col("id").desc())
)
