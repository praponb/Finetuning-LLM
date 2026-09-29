from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("duration_sec", "id")
w2 = Window.partitionBy("event_type").orderBy("duration_sec", "id")

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "duration_sec",
        "event_type",
        F.row_number().over(w).alias("rn"),
        F.row_number().over(w2).alias("duration_sec_row"),
    )
    .orderBy(F.col("duration_sec_row").desc())
)
