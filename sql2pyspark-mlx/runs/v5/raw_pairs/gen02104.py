from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy("event_time", "id")

enriched = (
    df_web_events
    .filter(F.col("event_type").isin(["purchase", "search", "page_view"]))
    .select(
        "id",
        "session_id",
        F.avg("duration_sec").over(w_running).alias("last7_avg_duration_sec"),
        F.min("duration_sec").over(w_running).alias("last7_min_duration_sec"),
        F.count("duration_sec").over(w_running).alias("last7_count_duration_sec"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

result = (
    enriched
    .select(
        "id",
        "last7_avg_duration_sec",
        "last7_min_duration_sec",
        "last7_count_duration_sec",
        "row_seq",
    )
)
