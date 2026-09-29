from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("t"), F.col("s.id") == F.col("t.session_id"), "left")
    .filter(F.col("t.max_duration_sec") > 360)
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("t.page_values"),
        F.col("t.max_duration_sec"),
    )
)
