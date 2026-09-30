from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_duration_sec").desc())

result = (
    df_web_events
    .filter(F.col("session_id").isin([17, 28, 38]))
    .groupBy(F.hour("event_time").alias("event_hour"))
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.count("*").alias("cnt"),
    )
    .withColumn("duration_sec_rank", F.dense_rank().over(w))
)
