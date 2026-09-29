from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.col("session_id").isin([26, 28, 35]))
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("page") == "/home", F.col("duration_sec")).otherwise(0)).alias("home_duration_sec"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_summary.alias("t"), F.col("sessions.id") == F.col("t.session_id"), "inner")
    .filter(F.col("t.home_duration_sec") > 75)
    .select(F.col("sessions.started_at"), F.col("t.home_duration_sec"))
    .orderBy("home_duration_sec")
)
