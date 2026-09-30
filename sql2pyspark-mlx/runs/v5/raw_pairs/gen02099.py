from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec", "id")
w_ordered = Window.partitionBy("session_id").orderBy("event_time", "id")
w3 = Window.partitionBy("session_id", "page").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

with_windows = (
    df_web_events
    .select(
        "id",
        "page",
        "event_time",
        F.row_number().over(w).alias("event_seq"),
        F.lag("duration_sec", 2).over(w_ordered).alias("prev_duration_sec"),
        F.avg("duration_sec").over(w3).alias("duration_sec_avg_30d"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "event_time",
        "event_seq",
        "prev_duration_sec",
        "duration_sec_avg_30d",
    )
    .orderBy(F.col("event_seq").desc_nulls_last())
)
