from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.browser"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.min(F.col("we.duration_sec")).alias("min_duration_sec"),
        F.sort_array(F.collect_set(F.col("we.event_type"))).alias("event_type_list"),
    )
)
