from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_id").orderBy(F.col("session_start_flag").desc_nulls_first(), "id")

prev_events = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

flagged = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_web_events = (
    flagged
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        F.sum("is_new_burst").over(w2).alias("island_id"),
    )
)

with_gaps = (
    sessions_web_events
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

gaps = (
    islands
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_web_events_2 = gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

latest = (
    sessions_web_events_2
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

cleaned = (
    latest
    .filter((F.col("event_time") > F.lit("2023-05-06").cast("date")) & (F.col("burst_id") < 3))
    .select(
        "*",
        F.datediff(F.current_date(), "event_time").alias("days_since_event_time"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.country").alias("country"))
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.avg(F.col("s.island_id")), 2).alias("avg_island_id"),
    )
)
