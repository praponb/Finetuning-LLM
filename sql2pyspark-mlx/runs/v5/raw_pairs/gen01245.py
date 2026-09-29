from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time")

result = (
    df_web_events
    .select("id", "session_id", "event_time", "page", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "session_id", "event_time", "page")
    .orderBy("session_id")
)
