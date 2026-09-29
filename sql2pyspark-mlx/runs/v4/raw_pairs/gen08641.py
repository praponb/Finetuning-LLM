from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec", "id")
w2 = Window.partitionBy("page", "session_id").orderBy("duration_sec", "id")
w3 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("session_id").orderBy("event_time", "id")
w5 = Window.partitionBy("page").orderBy("event_time", "id")
w6 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("session_id", "page").orderBy(F.col("session_start_flag").desc_nulls_first(), "id")

enriched = (
    df_web_events
    .withColumn("prev_duration_sec", F.lag("duration_sec", 2).over(w))
    .withColumn("next_duration_sec", F.lead("duration_sec", 1).over(w2))
)

with_windows = (
    enriched
    .withColumn("running_next_duration_sec", F.sum("next_duration_sec").over(w3))
    .withColumn("next_duration_sec_2", F.lead("duration_sec", 1).over(w_part))
)

gaps = (
    with_windows
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w5) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id_2", F.sum("session_start_flag").over(w6))

filtered = (
    sessionized
    .filter(F.col("event_time") > F.lit("2024-02-21 06:00:00").cast("timestamp"))
    .withColumn("event_time_label", F.date_format("event_time", "MMM yyyy"))
    .withColumn("running_next_duration_sec_whole", F.ceil("running_next_duration_sec"))
)

flagged = (
    filtered
    .withColumn("new_group_flag", F.when(F.col("event_time") - F.lag("event_time").over(w_part) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_web_events = flagged.withColumn("session_id_3", F.sum("new_group_flag").over(w7))

deduped = (
    sessions_web_events
    .withColumn("row_num", F.row_number().over(w8))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "prev_duration_sec",
        "next_duration_sec",
        "running_next_duration_sec",
        "next_duration_sec_2",
    )
    .orderBy(F.col("prev_duration_sec").desc_nulls_first())
)
