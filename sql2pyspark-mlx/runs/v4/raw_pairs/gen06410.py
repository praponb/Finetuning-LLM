from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("event_time").desc(), "id")

result = (
    df_web_events
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 5)
    .select("id", "session_id", "event_time", "event_type")
    .orderBy(F.col("session_id").desc(), "event_time")
)
