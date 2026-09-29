from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("event_time").asc_nulls_first(), "id")

deduped = (
    df_web_events
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .select("id", "event_type", "event_time", "duration_sec", "row_num")
    .orderBy(F.col("event_type").desc())
)
