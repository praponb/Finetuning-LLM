from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([13, 33, 36, 39]))
    .select("id", "event_type", F.floor("duration_sec").alias("duration_sec_whole"))
    .orderBy("id")
)
