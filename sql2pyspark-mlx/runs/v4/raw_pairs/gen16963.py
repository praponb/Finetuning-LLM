from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_web_events
    .withColumn("is_new_session", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

result = gaps.withColumn("session_id_2", F.sum("is_new_session").over(w2))
