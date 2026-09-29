from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_web_events
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        (F.col("duration_sec") - F.lag("duration_sec", 1).over(w)).alias("duration_sec_change"),
        F.last("event_time").over(w2).alias("last_event_time"),
    )
)
