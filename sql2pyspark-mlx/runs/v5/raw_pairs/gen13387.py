from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_web_events
    .withColumn("is_new_session", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        F.sum("is_new_session").over(w2).alias("session_id_2"),
    )
)

result = (
    sessionized
    .select("id", "session_id", "event_time", "session_id_2")
    .orderBy(F.col("event_time").desc(), "session_id")
)
