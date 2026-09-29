from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") < 187) & F.col("duration_sec").between(136, 179))
    .select("session_id", F.substring("event_type", 1, 4).alias("event_type_prefix"))
    .orderBy(F.col("event_type_prefix").desc())
)
