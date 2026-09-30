from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter((F.col("s.device") == "mobile") & (F.col("we.duration_sec") != 146))
    .select(
        F.col("s.country"),
        F.col("s.customer_id"),
        F.col("s.browser"),
        F.col("we.event_type"),
    )
)
