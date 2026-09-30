from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("we.duration_sec").between(357, 471))
    .groupBy(F.col("s.browser"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("we.event_type")))).alias("event_type_values"),
        F.round(F.stddev(F.col("we.duration_sec")), 2).alias("stddev_duration_sec"),
    )
    .orderBy("event_type_values")
)
