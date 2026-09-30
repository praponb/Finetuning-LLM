from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(~F.col("device").isin(["desktop", "mobile"])).alias("s"), F.col("we.session_id") == F.col("s.id"), "left_anti")
    .filter(F.col("we.page").isin(["/cart", "/checkout"]))
    .select(F.col("we.id"), F.col("we.duration_sec"), F.col("we.event_time"))
    .orderBy("duration_sec", F.col("event_time").desc())
)
