from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id")

result = (
    df_web_events
    .withColumn("avg_duration_sec_by_session", F.avg("duration_sec").over(w))
    .filter(F.col("duration_sec") > F.col("avg_duration_sec_by_session"))
    .select("id", "session_id", "duration_sec")
    .orderBy(F.col("session_id").desc())
)
