from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("event_time").desc())

result = (
    df_web_events
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 5)
    .select("id", "session_id", "event_time")
)
