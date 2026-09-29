from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("page").isin(["/checkout", "/product"]))
    .groupBy("session_id")
    .agg(
        F.countDistinct("duration_sec").alias("unique_pages"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("t"), F.col("s.id") == F.col("t.session_id"), "left")
    .filter(F.col("t.stddev_duration_sec") >= 64)
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("t.unique_pages"),
        F.col("t.stddev_duration_sec"),
    )
    .orderBy("browser")
)
