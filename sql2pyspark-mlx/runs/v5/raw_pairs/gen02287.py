from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.col("we.session_id").isin([11, 26, 27, 36]))
    .groupBy(F.col("s.browser"))
    .agg(F.countDistinct(F.col("we.duration_sec")).alias("unique_pages"))
    .orderBy(F.col("browser").desc(), "unique_pages")
    .limit(10)
)
