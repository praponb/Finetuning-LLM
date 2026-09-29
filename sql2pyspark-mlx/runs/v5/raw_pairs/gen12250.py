from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "outer")
    .filter((F.col("s.device") == "desktop") & F.col("s.customer_id").isin([3, 18, 19, 40]))
    .select(F.col("we.duration_sec"), F.col("s.device"), F.col("s.country"))
    .orderBy("country", F.col("device").desc())
)
