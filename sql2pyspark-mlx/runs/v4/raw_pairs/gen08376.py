from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("page").isin(["/search", "/checkout"]))
    .groupBy("session_id")
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.sum(F.when(F.col("event_type") == "click", F.col("duration_sec")).otherwise(0)).alias("click_duration_sec"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_stats.alias("session_stats"), F.col("sessions.id") == F.col("session_stats.session_id"), "inner")
    .filter(F.col("session_stats.avg_duration_sec") > 420)
    .select(
        F.col("sessions.browser"),
        F.col("session_stats.avg_duration_sec"),
        F.col("session_stats.click_duration_sec"),
    )
    .orderBy(F.col("avg_duration_sec").desc())
)
