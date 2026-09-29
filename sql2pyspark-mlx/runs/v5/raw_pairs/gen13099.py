from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("session_id").orderBy("duration_sec", "id")
w3 = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")
w4 = Window.partitionBy("page", "session_id").orderBy("event_time", "id")

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        F.sum("duration_sec").over(w).alias("running_duration_sec"),
        F.ntile(5).over(w2).alias("duration_sec_bucket"),
        F.ntile(10).over(w3).alias("duration_sec_bucket_2"),
        F.row_number().over(w4).alias("row_seq"),
        F.first("duration_sec", ignorenulls=True).over(w).alias("first_known_duration_sec"),
    )
)
