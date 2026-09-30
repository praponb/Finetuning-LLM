from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page")
w2 = Window.partitionBy("session_id", "event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_web_events
    .withColumn("page_sum_duration_sec", F.sum("duration_sec").over(w))
    .withColumn("cumulative_duration_sec", F.sum("duration_sec").over(w2))
)
