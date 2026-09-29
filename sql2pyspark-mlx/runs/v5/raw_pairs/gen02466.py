from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(F.col("started_at") >= "2023-08-01").alias("s"), F.col("we.session_id") == F.col("s.id"), "left_semi")
    .filter(F.col("we.page").isin(["/product", "/search", "/checkout"]))
    .select(F.col("we.id"), F.col("we.event_type"))
    .orderBy(F.col("id").desc())
)
