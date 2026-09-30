from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "session_id").orderBy("duration_sec", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_web_events
    .filter(F.col("session_id").isin([11, 31, 38]))
    .select(
        "id",
        "session_id",
        "event_type",
        "duration_sec",
        F.min("duration_sec").over(w).alias("last3_min_duration_sec"),
        F.count("duration_sec").over(w).alias("last3_count_duration_sec"),
        F.max("duration_sec").over(w).alias("last3_max_duration_sec"),
        F.avg("duration_sec").over(w).alias("last3_avg_duration_sec"),
    )
)
