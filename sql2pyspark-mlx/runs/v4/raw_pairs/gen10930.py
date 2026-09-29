from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "inner")
    .filter(F.col("s.country").isin(["SG", "UK", "JP"]))
    .select(
        F.col("s.started_at"),
        F.col("s.browser"),
        F.col("we.session_id"),
        F.col("we.event_type"),
    )
    .orderBy("event_type", F.col("started_at").desc())
)
