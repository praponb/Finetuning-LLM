from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("duration_sec", "id")
w2 = Window.partitionBy("page", "event_type").orderBy("duration_sec", "id")

scored = (
    df_web_events
    .select(
        "id",
        "session_id",
        "page",
        "duration_sec",
        F.ntile(5).over(w).alias("duration_sec_bucket"),
        F.ntile(3).over(w2).alias("duration_sec_bucket_2"),
    )
)

result = (
    scored
    .select(
        "id",
        "session_id",
        "duration_sec",
        "duration_sec_bucket",
        "duration_sec_bucket_2",
    )
    .orderBy(F.col("duration_sec").desc_nulls_first(), F.col("id").desc())
)
