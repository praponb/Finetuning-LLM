from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.to_date("event_time") < "2024-04-15")
    .groupBy("session_id")
    .agg(F.sort_array(F.collect_set("page")).alias("page_list"))
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.started_at"),
        F.col("webs.page_list"),
    )
)
