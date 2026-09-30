from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_web_events
    .filter(F.col("session_id").isin([14, 17, 24]))
    .select(
        "id",
        "page",
        "duration_sec",
        "event_type",
        F.last("event_type").over(w).alias("last_event_type"),
    )
)
