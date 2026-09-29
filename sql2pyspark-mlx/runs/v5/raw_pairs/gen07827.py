from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_web_events
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_web_events = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_web_events
    .select(
        "id",
        "event_type",
        "page",
        "duration_sec",
        "session_start_flag",
        "island_id",
    )
    .orderBy(F.col("session_start_flag").desc_nulls_first())
)
