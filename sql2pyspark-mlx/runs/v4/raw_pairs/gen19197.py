from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("duration_sec", "id")

result = (
    df_web_events
    .filter(F.col("event_type").isin(["click", "search"]))
    .select("id", "session_id", "duration_sec", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
)
