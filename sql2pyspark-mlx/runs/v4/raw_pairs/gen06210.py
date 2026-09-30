from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .select(
        F.col("s.browser"),
        F.col("s.started_at"),
        F.col("s.country"),
        F.col("we.event_time"),
        F.col("we.page"),
        F.col("we.event_type"),
        F.coalesce(F.col("we.page"), F.lit("unassigned")).alias("page_filled"),
    )
    .orderBy("browser")
)
