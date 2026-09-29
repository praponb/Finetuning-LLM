from pyspark.sql import functions as F

session_stats = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .groupBy("session_id")
    .agg(
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_stats.alias("session_stats"), F.col("sessions.id") == F.col("session_stats.session_id"), "left")
    .filter(F.col("session_stats.min_duration_sec") > 400)
    .select(
        F.col("sessions.country"),
        F.col("session_stats.page_list"),
        F.col("session_stats.min_duration_sec"),
    )
    .orderBy(F.col("min_duration_sec").desc())
)
