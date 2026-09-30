from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "page").orderBy(F.col("duration_sec").asc_nulls_last(), "id")

latest = (
    df_web_events
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .select("id", "page", "event_time", "duration_sec", "rank_in_group")
    .orderBy(F.col("id").desc_nulls_last(), F.col("rank_in_group").desc())
    .limit(20)
)
