from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time")

result = (
    df_web_events
    .filter(F.col("duration_sec") <= 149)
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") <= 3)
    .select("id", "session_id", "event_time")
)
