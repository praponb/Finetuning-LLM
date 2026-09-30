from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w_ordered = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        F.lead("duration_sec", 1).over(w).alias("next_duration_sec"),
        F.sum("duration_sec").over(w_ordered).alias("running_duration_sec"),
    )
)
