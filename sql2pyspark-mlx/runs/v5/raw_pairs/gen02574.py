from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.country"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.sum(F.col("we.duration_sec")).alias("total_duration_sec"),
        F.max(F.col("we.duration_sec")).alias("max_duration_sec"),
    )
    .filter(F.col("total_duration_sec") > 2600)
    .orderBy("total_duration_sec", "country")
    .limit(20)
)
