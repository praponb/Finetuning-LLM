from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("duration_sec", "id")
w2 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_running = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("page").orderBy("duration_sec", "id")

result = (
    df_web_events
    .withColumn("next_duration_sec", F.lead("duration_sec", 1, 0).over(w))
    .withColumn("duration_sec_count_7d", F.count("duration_sec").over(w2))
    .withColumn("cumulative_duration_sec", F.sum("duration_sec").over(w_running))
    .withColumn("duration_sec_bucket", F.ntile(3).over(w_ordered))
)
