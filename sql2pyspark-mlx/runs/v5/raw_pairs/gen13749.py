from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")

deduped = (
    df_web_events
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "event_type", "duration_sec")
)

result = (
    deduped
    .select("id", "duration_sec")
    .orderBy(F.col("duration_sec").desc())
)
