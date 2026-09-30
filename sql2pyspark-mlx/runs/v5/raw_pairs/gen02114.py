from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("duration_sec")
w2 = Window.partitionBy("page").orderBy(F.col("duration_sec").asc_nulls_first())
w3 = Window.partitionBy("event_type").orderBy("event_time", "id")
w4 = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("page", "session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        F.rank().over(w).alias("duration_sec_rank"),
        F.dense_rank().over(w2).alias("duration_sec_dense_rank"),
        F.lag("duration_sec", 1).over(w3).alias("prev_duration_sec"),
        F.sum("duration_sec").over(w4).alias("running_duration_sec"),
        F.max("duration_sec").over(w_grp).alias("duration_sec_max_30d"),
    )
)
