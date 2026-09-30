from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.col("duration_sec") == 377).alias("we"), F.col("s.id") == F.col("we.session_id"), "left_semi")
    .filter(F.col("s.country").isin(["DE", "JP", "US"]))
    .select(F.col("s.id"), F.col("s.country"))
)
