from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id")
w2 = Window.partitionBy("event_type", "page").orderBy("duration_sec", "id")
w3 = Window.partitionBy("session_id", "event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(-5, -1)

result = (
    df_web_events
    .filter(F.col("duration_sec").between(130, 295))
    .select(
        "id",
        "event_type",
        F.min("duration_sec").over(w).alias("session_id_min_duration_sec"),
        F.row_number().over(w2).alias("seq_no"),
        F.last("duration_sec", ignorenulls=True).over(w3).alias("last_known_duration_sec"),
        F.first("duration_sec", ignorenulls=True).over(w_recent).alias("first_known_duration_sec"),
        F.avg("duration_sec").over(w_running).alias("duration_sec_avg_5_prior"),
    )
)
