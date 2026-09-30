from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.country"))
    .agg(F.max(F.col("we.duration_sec")).alias("max_duration_sec"))
    .filter(F.col("max_duration_sec") > 310)
    .orderBy(F.col("max_duration_sec").desc())
)
