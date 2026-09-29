from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_web_events
    .withColumn("new_group_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_web_events = (
    gaps
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.sum("new_group_flag").over(w2).alias("session_no"),
    )
)

result = (
    sessions_web_events
    .select("id", "event_type", "event_time", "session_no")
    .orderBy(F.col("id").desc())
)
