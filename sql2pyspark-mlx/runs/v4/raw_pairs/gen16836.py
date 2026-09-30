from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec", "id")
w2 = Window.partitionBy("event_type", "session_id").orderBy(F.col("duration_sec").desc_nulls_first())

with_windows = (
    df_web_events
    .withColumn("prev_duration_sec", F.lag("duration_sec", 1).over(w))
    .withColumn("duration_sec_rank", F.rank().over(w2))
)

result = (
    with_windows
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "prev_duration_sec",
        "duration_sec_rank",
    )
    .orderBy(F.col("page").desc())
)
