from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

flagged = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id_2", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        "prev_event_time",
        "session_id_2",
    )
    .orderBy("duration_sec", "id")
    .limit(10)
)
