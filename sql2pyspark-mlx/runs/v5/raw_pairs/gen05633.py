from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.device"))
    .agg(
        F.sort_array(F.collect_set(F.col("we.event_type"))).alias("event_type_list"),
        F.sum(F.col("we.duration_sec")).alias("total_duration_sec"),
    )
)
