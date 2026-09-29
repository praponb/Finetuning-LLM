from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")

result = (
    df_web_events
    .select("id", "page", "event_time", F.ntile(4).over(w).alias("event_time_ntile"))
)
