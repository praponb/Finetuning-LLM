from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")

result = (
    df_web_events
    .filter(F.col("session_id").isin([15, 24]))
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "page", "event_time", "session_id")
    .orderBy(F.col("session_id").desc(), "event_time")
)
