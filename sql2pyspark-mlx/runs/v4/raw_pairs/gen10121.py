from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.country"))
    .agg(
        F.count(F.col("we.duration_sec")).alias("duration_sec_count"),
        F.avg(F.col("we.duration_sec")).alias("avg_duration_sec"),
    )
    .orderBy(F.col("duration_sec_count").desc())
)
