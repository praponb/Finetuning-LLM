from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("page").isin(["/product", "/cart"]))
    .groupBy("session_id")
    .agg(F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "left")
    .filter(F.col("webs.stddev_duration_sec") >= 62)
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("s.customer_id"),
        F.col("webs.stddev_duration_sec"),
    )
)
