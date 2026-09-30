from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("event_time", "id")

result = (
    df_web_events
    .select("id", "event_time", F.ntile(3).over(w).alias("quartile"))
    .orderBy("event_time")
)
