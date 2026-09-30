from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("session_id").isin([7, 26, 40]))
    .groupBy("session_id")
    .agg(F.sum(F.when(F.col("page") == "/home", 1).otherwise(0)).alias("home_count"))
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("S"), F.col("s.id") == F.col("S.session_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.started_at"),
        F.col("S.home_count"),
    )
    .orderBy("started_at")
)
