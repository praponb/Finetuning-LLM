from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("t"), F.col("s.id") == F.col("t.session_id"), "left")
    .filter(F.col("t.max_duration_sec") > 420)
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("s.browser"),
        F.col("t.max_duration_sec"),
        F.col("t.stddev_duration_sec"),
    )
)
