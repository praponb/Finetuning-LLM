from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec", "id")
w2 = Window.orderBy("duration_sec", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("page").orderBy("duration_sec_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("session_id")
w5 = Window.partitionBy("session_id").orderBy("event_time", "id")
w6 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.max("duration_sec").alias("max_web_event_duration_sec"))

filtered = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-04-20 20:00:00").cast("timestamp"))
    .withColumn("page_lower", F.lower("page"))
    .withColumn("page_clean", F.coalesce("page", F.lit("UNKNOWN")))
)

with_windows = (
    filtered
    .filter(F.col("event_time") >= "2024-05-26")
    .select(
        "*",
        F.ntile(10).over(w).alias("duration_sec_bucket"),
        F.avg("duration_sec").over(w2).alias("duration_sec_avg_2"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        "page_lower",
        "duration_sec_bucket",
        "duration_sec_avg_2",
        F.sum("duration_sec").over(w3).alias("cumulative_duration_sec"),
        F.avg("duration_sec_avg_2").over(w4).alias("session_id_avg_duration_sec_avg_2"),
    )
)

joined = (
    scored
    .alias("s")
    .join(df_sessions.alias("p2"), F.col("s.session_id") == F.col("p2.id"), "inner")
    .select("s.*", F.col("p2.country"))
)

enriched = (
    joined
    .alias("e")
    .join(df_sessions.alias("b"), F.col("e.session_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.device"))
)

with_gaps = (
    enriched
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w5).isNull() | (F.col("event_time") > F.lag("event_time").over(w5) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec_bucket",
        "session_id_avg_duration_sec_avg_2",
        F.sum("is_new_session").over(w6).alias("burst_id"),
    )
)

cleaned = (
    islands
    .filter(F.col("event_time") >= F.to_date(F.lit("2023-04-13")))
    .withColumn("event_time_quarter", F.quarter("event_time"))
)

result = (
    cleaned
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "duration_sec_bucket",
        "burst_id",
        "event_time_quarter",
        F.col("max_web_event_duration_sec"),
    )
    .orderBy(F.col("burst_id").asc_nulls_first())
)
