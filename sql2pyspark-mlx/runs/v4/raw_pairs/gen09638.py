from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")

result = (
    df_web_events
    .filter(F.col("session_id").isin([7, 33]))
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        (F.col("duration_sec") - F.lag("duration_sec", 2).over(w)).alias("duration_sec_change"),
    )
    .orderBy(F.col("id").desc())
)
