from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec", "id")

result = (
    df_web_events
    .filter(F.col("event_time") >= "2023-11-01")
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 2)
    .select("id", "page", "duration_sec", "event_time")
    .orderBy("event_time")
)
