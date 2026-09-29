from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("we.session_id") == 39)
    .select(F.col("s.country"), F.col("s.started_at"), F.col("we.session_id"))
    .orderBy(F.col("session_id").desc())
)
