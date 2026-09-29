from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id")

result = (
    df_web_events
    .withColumn("min_duration_sec_by_session", F.min("duration_sec").over(w))
    .filter(F.col("duration_sec") == F.col("min_duration_sec_by_session"))
    .select("id", "session_id", "duration_sec", "event_type")
)
