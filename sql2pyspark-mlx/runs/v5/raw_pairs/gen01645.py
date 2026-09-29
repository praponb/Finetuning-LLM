from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_web_events
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_id_2", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .select("id", "event_time", "duration_sec", "session_id_2")
    .orderBy(F.col("session_id_2").desc_nulls_last())
)
