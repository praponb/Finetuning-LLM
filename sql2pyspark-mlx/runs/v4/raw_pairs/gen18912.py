from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "page")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_web_events
    .withColumn("event_type_count_duration_sec", F.count("duration_sec").over(w))
    .withColumn("duration_sec_avg_6_prior", F.avg("duration_sec").over(w2))
    .withColumn("cumulative_duration_sec", F.sum("duration_sec").over(w3))
)
