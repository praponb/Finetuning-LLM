from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type")

result = (
    df_web_events
    .filter(F.col("duration_sec") <= 358)
    .select(
        "id",
        "page",
        "event_time",
        "event_type",
        "duration_sec",
        F.row_number().over(w).alias("rn"),
        F.sum("duration_sec").over(w2).alias("event_type_sum_duration_sec"),
    )
    .orderBy("event_time")
)
