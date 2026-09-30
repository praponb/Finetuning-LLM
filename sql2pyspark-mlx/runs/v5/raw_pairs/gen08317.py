from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("page", "session_id").orderBy(F.col("duration_sec").desc())

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        F.last("duration_sec", ignorenulls=True).over(w).alias("last_known_duration_sec"),
        F.dense_rank().over(w2).alias("duration_sec_dense_rank"),
    )
)
