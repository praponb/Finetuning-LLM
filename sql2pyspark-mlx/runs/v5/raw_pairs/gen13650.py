from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "right")
    .filter(~F.col("s.browser").isin(["Safari", "Firefox"]) & F.col("we.page").isin(["/checkout", "/home", "/product"]))
    .select(
        F.col("we.event_time"),
        F.col("s.browser"),
        F.col("s.device"),
        F.col("s.started_at"),
    )
    .orderBy(F.col("device").desc(), "event_time")
)
