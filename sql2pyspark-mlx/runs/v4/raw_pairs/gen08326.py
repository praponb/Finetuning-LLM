from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.browser") == "Chrome")
    .select(F.col("we.session_id"), F.col("s.country"))
    .orderBy(F.col("session_id").desc())
)
