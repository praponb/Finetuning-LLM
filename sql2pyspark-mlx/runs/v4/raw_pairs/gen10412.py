from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("session_id").isin([4, 24, 27, 38]))
    .groupBy("session_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0))) / F.count("*")), 1).alias("pct_click"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .select(F.col("s.device"), F.col("s.country"), F.col("t.pct_click"))
    .orderBy(F.col("device").desc())
)
