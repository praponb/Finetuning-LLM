from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "event_type").orderBy(F.col("duration_sec").desc_nulls_first(), "id")

result = (
    df_web_events
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "session_id", "event_type", "event_time")
)
