from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("duration_sec", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id")
w_part_2 = Window.partitionBy("session_id").orderBy("duration_sec", "id")
w4 = Window.partitionBy("session_id", "event_type").orderBy(F.col("duration_sec").asc_nulls_last())

cleaned = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.round("duration_sec", 1).alias("duration_sec_rounded"),
        F.to_date("event_time").alias("event_time_date"),
    )
)

result = (
    cleaned
    .filter(F.col("session_id").isin([4, 5, 16, 25]))
    .select(
        "id",
        "event_type",
        "duration_sec_rounded",
        F.ntile(5).over(w_part).alias("duration_sec_bucket"),
        F.row_number().over(w2).alias("seq_no"),
        F.ntile(10).over(w_part_2).alias("duration_sec_bucket_2"),
        F.dense_rank().over(w4).alias("duration_sec_dense_rank"),
    )
)
