from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type")
w3 = Window.partitionBy("session_id", "page").orderBy(F.col("duration_sec").desc(), "id")
w4 = Window.orderBy("duration_sec", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("event_type", "page").orderBy("duration_sec", "id")

filtered = (
    df_web_events
    .filter(F.col("event_time") > F.to_date(F.lit("2023-06-08")))
    .select("*", F.col("event_time").cast("date").alias("event_time_day"))
)

result = (
    filtered
    .filter(F.col("session_id") == 2)
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.lead("duration_sec", 2, 0).over(w).alias("next_duration_sec"),
        F.count("duration_sec").over(w2).alias("event_type_count_duration_sec"),
        F.row_number().over(w3).alias("event_seq"),
        F.avg("duration_sec").over(w4).alias("duration_sec_avg_3"),
        F.ntile(5).over(w5).alias("duration_sec_bucket"),
    )
)
