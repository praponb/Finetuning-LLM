from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_web_events
    .filter(F.col("page") == "/checkout")
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        F.last("duration_sec").over(w).alias("last_duration_sec"),
    )
)
