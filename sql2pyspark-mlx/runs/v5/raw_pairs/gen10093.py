from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy(F.col("event_time").desc(), "id")

result = (
    df_web_events
    .filter(F.col("event_time") < "2023-07-01")
    .select(
        "id",
        "page",
        "event_type",
        "event_time",
        "duration_sec",
        F.lead("duration_sec", 1).over(w).alias("next_duration_sec"),
        F.row_number().over(w2).alias("row_num"),
    )
)
