from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "page").orderBy(F.col("event_time").desc(), "id")

deduped = (
    df_web_events
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .select("id", "session_id", "event_type", "page", "duration_sec", "dedup_rank")
)
