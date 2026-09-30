from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type")
w2 = Window.partitionBy("page").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

scored = (
    df_web_events
    .filter(F.col("duration_sec") != 196)
    .select(
        "id",
        "page",
        "event_time",
        F.max("duration_sec").over(w).alias("event_type_max_duration_sec"),
        F.count("duration_sec").over(w2).alias("duration_sec_count_30d"),
    )
)

result = (
    scored
    .select("id", "event_time", "event_type_max_duration_sec", "duration_sec_count_30d")
)
