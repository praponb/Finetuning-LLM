from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "session_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)

result = (
    df_web_events
    .filter(F.to_date("event_time") < "2025-08-01")
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.count("duration_sec").over(w).alias("running_count_duration_sec"),
        F.avg("duration_sec").over(w).alias("running_avg_duration_sec"),
        F.max("duration_sec").over(w).alias("running_max_duration_sec"),
        F.sum("duration_sec").over(w2).alias("duration_sec_sum_7d"),
    )
)
