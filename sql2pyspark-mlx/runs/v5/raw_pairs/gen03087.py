from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("duration_sec").alias("unique_pages"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "left")
    .filter(F.col("webs.row_count") > 6)
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("s.device"),
        F.col("webs.row_count"),
        F.col("webs.unique_pages"),
    )
    .orderBy("started_at", F.col("row_count").desc())
)
