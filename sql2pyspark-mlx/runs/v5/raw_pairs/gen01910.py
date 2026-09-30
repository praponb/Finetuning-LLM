from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "event_type").orderBy(F.col("duration_sec").desc_nulls_last(), "id")

filtered = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-09-14").cast("date")) & ~F.col("page").isin(["/search", "/checkout", "/home"]))
    .withColumn("clean_event_type", F.trim("event_type"))
)

result = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "session_id", "event_type", "duration_sec")
)
