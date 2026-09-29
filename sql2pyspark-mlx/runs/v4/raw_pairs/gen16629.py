from pyspark.sql import functions as F

valid_web_events = df_web_events.filter(F.col("page") == "/product")

web_events_stats = (
    valid_web_events
    .groupBy("session_id")
    .agg(F.count("*").alias("cnt"))
)

result = (
    web_events_stats
    .filter(F.col("cnt") > 4)
    .select("session_id", "cnt")
    .orderBy(F.col("session_id").desc())
)
