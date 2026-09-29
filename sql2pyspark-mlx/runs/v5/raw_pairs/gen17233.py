from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page").orderBy(F.col("island_id").desc(), "id")
w4 = Window.partitionBy("session_id").orderBy("event_time", "id")
w5 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("session_id").orderBy(F.col("event_time").asc_nulls_last(), "id")
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.min("duration_sec").alias("min_web_event_duration_sec"))

gaps = (
    df_web_events
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_web_events = (
    gaps
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        F.sum("session_start_flag").over(w2).alias("island_id"),
    )
)

cleaned = (
    sessions_web_events
    .filter(F.hour("event_time").between(10, 13))
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "island_id",
        F.weekofyear("event_time").alias("event_time_weekofyear"),
        F.coalesce("page", F.lit("OTHER")).alias("page_filled"),
    )
)

deduped = (
    cleaned
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

gaps_2 = (
    deduped
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w4).isNull() | (F.col("event_time") > F.lag("event_time").over(w4) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = (
    gaps_2
    .select(
        "id",
        "session_id",
        "event_time",
        "island_id",
        F.sum("is_new_session").over(w5).alias("island_id_2"),
    )
)

deduped_web_events = (
    islands
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped_web_events
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_time",
        "island_id_2",
        "latest_rank",
        F.col("min_web_event_duration_sec"),
    )
    .orderBy(F.col("latest_rank").desc(), "id")
)
