from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy("duration_sec", "id")
w4 = Window.partitionBy("page", "session_id").orderBy("duration_sec", "id")
w5 = Window.partitionBy("event_type").orderBy("burst_id_plus_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.sum("duration_sec").alias("sum_web_event_duration_sec"))

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select("*", F.col("duration_sec").cast("double").alias("duration_sec_str"))
)

gaps = (
    filtered
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

with_gaps = (
    islands
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_web_events = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

cleaned = (
    sessions_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "*",
        (F.col("burst_id") + F.col("duration_sec")).alias("burst_id_plus_duration_sec"),
        (F.col("island_id") - F.col("is_new_burst")).alias("island_id_minus_is_new_burst"),
    )
)

with_windows = (
    cleaned
    .withColumn("duration_sec_bucket", F.ntile(5).over(w3))
    .withColumn("prev_burst_id", F.lag("burst_id", 2).over(w4))
    .withColumn("running_is_new_burst", F.sum("is_new_burst").over(w5))
)

with_gaps_2 = (
    with_windows
    .withColumn("is_new_burst_2", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_web_events_2 = with_gaps_2.withColumn("session_id_2", F.sum("is_new_burst_2").over(w2))

result = (
    sessions_web_events_2
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec_str",
        "island_id",
        "is_new_burst",
        "burst_id_plus_duration_sec",
        "island_id_minus_is_new_burst",
        F.col("sum_web_event_duration_sec"),
    )
    .orderBy("session_id")
)
