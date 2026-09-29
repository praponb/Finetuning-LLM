from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "event_type").orderBy("event_time", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("page").orderBy(F.col("duration_sec").desc_nulls_last())

result = (
    df_web_events
    .withColumn("duration_sec_avg_2", F.avg("duration_sec").over(w))
    .withColumn("duration_sec_percent_rank", F.percent_rank().over(w2))
)
