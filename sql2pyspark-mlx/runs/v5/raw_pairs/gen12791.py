from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.country"), F.col("s.device"))
    .agg(F.min(F.col("we.duration_sec")).alias("min_duration_sec"))
    .filter(F.col("min_duration_sec") > 370)
    .orderBy(F.col("country").desc(), F.col("device").desc())
)
