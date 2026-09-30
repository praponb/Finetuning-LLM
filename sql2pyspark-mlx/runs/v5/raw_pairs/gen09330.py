from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("event_day")

result = (
    df_web_events
    .filter(F.col("duration_sec") < 457)
    .groupBy(F.to_date("event_time").alias("event_day"))
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_duration_sec", F.sum("total_duration_sec").over(w))
    .orderBy("cumulative_duration_sec")
)
