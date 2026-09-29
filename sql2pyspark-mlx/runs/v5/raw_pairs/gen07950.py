from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc(), "id")
w3 = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")
w_part = Window.partitionBy("session_id").orderBy("duration_sec", "id")
w5 = Window.partitionBy("page").orderBy(F.col("event_time").desc(), "id")

result = (
    df_web_events
    .filter(F.col("page").isin(["/product", "/home"]))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.sum("duration_sec").over(w_recent).alias("running_duration_sec"),
        F.row_number().over(w2).alias("event_seq"),
        F.ntile(5).over(w3).alias("duration_sec_bucket"),
        F.ntile(4).over(w_part).alias("duration_sec_bucket_2"),
        F.row_number().over(w5).alias("event_seq_2"),
    )
)
