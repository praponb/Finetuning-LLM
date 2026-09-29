from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("event_hour")

result = (
    df_web_events
    .filter(F.col("duration_sec") != 173)
    .groupBy(F.hour("event_time").alias("event_hour"))
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
    .withColumn("cumulative_duration_sec", F.sum("total_duration_sec").over(w))
)
