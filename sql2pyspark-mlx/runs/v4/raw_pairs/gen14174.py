from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_web_events
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

result = flagged.withColumn("burst_id", F.sum("session_start_flag").over(w2))
