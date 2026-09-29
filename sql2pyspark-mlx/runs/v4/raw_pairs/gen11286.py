from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time")

result = (
    df_web_events
    .select("id", "page", "event_time", F.rank().over(w).alias("event_time_rank"))
)
