from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_web_events
    .select(
        "id",
        "duration_sec",
        "event_time",
        F.last("event_time").over(w).alias("last_event_time"),
    )
)
