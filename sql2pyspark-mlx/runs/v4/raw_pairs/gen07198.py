from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_web_events
    .withColumn("event_type_min_duration_sec", F.min("duration_sec").over(w))
    .withColumn("last_known_duration_sec", F.last("duration_sec", ignorenulls=True).over(w2))
)

result = (
    with_windows
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "event_type_min_duration_sec",
        "last_known_duration_sec",
    )
    .orderBy(F.col("id").desc_nulls_last())
    .limit(10)
)
