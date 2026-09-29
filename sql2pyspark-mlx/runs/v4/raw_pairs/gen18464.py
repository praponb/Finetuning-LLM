from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_web_events
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("session_id_2", F.sum("is_new_burst").over(w2))

result = (
    islands
    .select("id", "page", "event_time", "duration_sec", "is_new_burst")
    .orderBy(F.col("is_new_burst").desc_nulls_first())
)
