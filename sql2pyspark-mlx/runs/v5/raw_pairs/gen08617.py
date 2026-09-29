from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("duration_sec", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id")

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "duration_sec",
        "event_type",
        "event_time",
        F.row_number().over(w).alias("duration_sec_row"),
        F.lead("duration_sec", 2).over(w2).alias("next_duration_sec"),
    )
    .orderBy(F.col("duration_sec_row").desc(), F.col("id").desc())
)
