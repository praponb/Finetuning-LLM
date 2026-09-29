from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.col("duration_sec") != 138).alias("we"), F.col("s.id") == F.col("we.session_id"), "left_anti")
    .select(F.col("s.id"), F.col("s.started_at"))
)
