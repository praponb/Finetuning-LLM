from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("event_time").desc())

result = (
    df_web_events
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") <= 2)
    .orderBy(F.col("duration_sec").desc(), F.col("session_id").desc())
)
