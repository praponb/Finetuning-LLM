from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(~F.col("s.country").isin(["TH", "US", "DE"]))
    .select(
        F.col("we.id"),
        F.col("we.page"),
        F.col("we.event_type"),
        F.col("s.browser"),
        F.col("s.customer_id"),
        F.col("s.device"),
    )
)
