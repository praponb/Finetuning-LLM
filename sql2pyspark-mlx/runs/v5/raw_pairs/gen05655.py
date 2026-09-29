from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "right")
    .filter((F.col("we.page") == "/search") & F.col("we.page").isin(["/cart", "/search", "/home"]))
    .select(
        F.col("we.id"),
        F.col("we.event_type"),
        F.col("we.page"),
        F.col("we.event_time"),
        F.col("s.browser"),
    )
)
