from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .groupBy(F.month(F.col("s.started_at")).alias("started_month"), F.col("s.country"))
    .agg(F.min(F.col("we.duration_sec")).alias("min_duration_sec"))
    .orderBy("country")
)
