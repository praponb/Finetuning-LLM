from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(F.col("device").isin(["mobile", "desktop"])).alias("s"), F.col("we.session_id") == F.col("s.id"), "left_semi")
    .filter(F.col("we.session_id") == 22)
    .select(F.col("we.id"), F.col("we.event_time"), F.col("we.page"))
    .orderBy(F.col("id").desc())
)
