from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

with_gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

result = (
    with_gaps
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        F.sum("is_new_session").over(w2).alias("session_id_2"),
    )
)
