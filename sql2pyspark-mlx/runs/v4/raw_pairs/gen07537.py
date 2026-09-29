from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("session_id")

result = (
    df_web_events
    .filter(F.col("event_type").isin(["purchase", "page_view"]))
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        F.sum("duration_sec").over(w).alias("running_duration_sec"),
        F.max("duration_sec").over(w2).alias("session_id_max_duration_sec"),
    )
)
