from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("we.page").isin(["/cart", "/home"]))
    .select(
        F.col("we.id"),
        F.col("we.session_id"),
        F.col("we.page"),
        F.col("s.country"),
        F.col("s.browser"),
        F.col("s.started_at"),
    )
    .orderBy(F.col("session_id").desc())
)
