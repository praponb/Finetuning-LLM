from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.col("we.event_type").isin(["click", "page_view"]) & F.col("s.customer_id").isNotNull())
    .select(F.col("s.started_at"), F.col("we.session_id"), F.col("we.duration_sec"))
    .orderBy(F.col("session_id").desc())
)
