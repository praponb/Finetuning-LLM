from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("we.duration_sec") > 298)
    .select(
        F.col("we.duration_sec"),
        F.col("we.event_time"),
        F.col("we.session_id"),
        F.col("s.browser"),
        F.col("s.customer_id"),
        F.coalesce(F.col("we.page"), F.lit("unassigned")).alias("page_filled"),
    )
    .orderBy("event_time")
)
