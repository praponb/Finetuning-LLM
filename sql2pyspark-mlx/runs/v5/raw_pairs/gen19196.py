from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("duration_sec").desc(), "id")

result = (
    df_web_events
    .filter(F.col("session_id") == 39)
    .select(
        "id",
        "session_id",
        "event_type",
        F.count("duration_sec").over(w).alias("running_count_duration_sec"),
        F.max("duration_sec").over(w).alias("running_max_duration_sec"),
        F.avg("duration_sec").over(w).alias("running_avg_duration_sec"),
        F.sum("duration_sec").over(w).alias("running_sum_duration_sec"),
        F.row_number().over(w2).alias("seq_no"),
    )
)
