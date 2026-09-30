from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("page")

result = (
    df_web_events
    .filter(F.col("event_time") >= "2025-07-01")
    .groupBy("page")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_duration_sec", F.sum("total_duration_sec").over(w))
    .orderBy(F.col("cumulative_duration_sec").desc())
)
