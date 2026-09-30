from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(~F.col("country").isin(["US", "SG"])).alias("s"), F.col("we.session_id") == F.col("s.id"), "left_semi")
    .filter(F.col("we.page") == "/cart")
    .select(
        F.col("we.id"),
        F.col("we.event_type"),
        F.col("we.session_id"),
        F.col("we.event_time"),
    )
    .orderBy(F.col("id").desc())
    .limit(1)
)
