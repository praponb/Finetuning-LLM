from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "event_type").orderBy(F.col("duration_sec").asc_nulls_last(), "id")

deduped = (
    df_web_events
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = deduped.select("id", "session_id", "page", "event_time", "rn")
