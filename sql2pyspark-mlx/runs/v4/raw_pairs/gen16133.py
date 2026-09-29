from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_web_events
    .withColumn("duration_sec_avg_5", F.avg("duration_sec").over(w_running))
    .withColumn("cumulative_duration_sec", F.sum("duration_sec").over(w2))
)
