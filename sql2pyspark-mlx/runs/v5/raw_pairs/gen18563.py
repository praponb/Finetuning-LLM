from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")

result = (
    df_web_events
    .filter(F.col("duration_sec") > 431)
    .select("id", "page", "event_time", F.row_number().over(w).alias("event_time_row"))
)
