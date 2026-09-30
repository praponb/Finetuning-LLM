from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "page").orderBy("duration_sec", "id")
w2 = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("session_id", "page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("session_id")
w_grp = Window.partitionBy("event_type", "page")

enriched = (
    df_web_events
    .select(
        "*",
        F.ntile(10).over(w).alias("duration_sec_bucket"),
        F.avg("duration_sec").over(w2).alias("duration_sec_avg_6"),
        F.sum("duration_sec").over(w3).alias("cumulative_duration_sec"),
        F.avg("duration_sec").over(w4).alias("session_id_avg_duration_sec"),
        F.min("duration_sec").over(w_grp).alias("event_type_min_duration_sec"),
    )
)

result = (
    enriched
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec_bucket",
        "duration_sec_avg_6",
        "cumulative_duration_sec",
    )
    .orderBy(F.col("id").desc_nulls_first())
)
