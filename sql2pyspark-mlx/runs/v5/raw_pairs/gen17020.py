from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "session_id").orderBy(F.col("duration_sec").desc(), "id")

result = (
    df_web_events
    .select(
        "id",
        "event_type",
        "session_id",
        "duration_sec",
        F.ntile(4).over(w).alias("duration_sec_ntile"),
    )
)
