from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("event_time")

result = (
    df_web_events
    .filter(F.col("event_type") == "page_view")
    .select("id", "event_time", F.rank().over(w).alias("event_time_rank"))
)
