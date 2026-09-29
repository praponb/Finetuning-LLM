from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("duration_sec") < 337)
    .groupBy("session_id")
    .agg(
        F.count(F.when(F.col("event_type") == "page_view", 1)).alias("page_view_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "inner")
    .filter(F.col("webs.page_view_count") > 6)
    .select(F.col("s.id"), F.col("s.customer_id"), F.col("webs.page_view_count"))
    .orderBy(F.col("id").desc())
)
