from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("duration_sec") > 160)
    .groupBy("session_id")
    .agg(F.sum(F.when(F.col("page") == "/home", 1).otherwise(0)).alias("home_count"))
)

result = (
    df_sessions.alias("s")
    .join(session_stats.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .select(F.col("s.id"), F.col("s.country"), F.col("t.home_count"))
    .orderBy("id")
)
