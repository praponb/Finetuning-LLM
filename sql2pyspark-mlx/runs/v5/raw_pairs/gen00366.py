from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")

result = (
    df_web_events
    .filter(F.col("duration_sec").isNotNull())
    .select("id", "session_id", "event_time", F.row_number().over(w).alias("rn"))
    .orderBy("event_time")
)
