from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "page",
        "duration_sec",
        F.avg("duration_sec").over(w).alias("duration_sec_avg_6"),
        F.last("duration_sec", ignorenulls=True).over(w2).alias("last_known_duration_sec"),
    )
)
