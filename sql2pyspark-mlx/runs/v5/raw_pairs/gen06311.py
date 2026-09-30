from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("duration_sec")

result = (
    df_web_events
    .filter(F.col("event_type") == "search")
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") <= 5)
    .select("id", "session_id", "duration_sec", "page")
    .orderBy(F.col("id").desc())
)
