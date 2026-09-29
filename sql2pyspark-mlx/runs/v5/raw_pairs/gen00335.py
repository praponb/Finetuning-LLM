from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("event_time")

result = df_web_events.select("id", "event_time", F.rank().over(w).alias("rnk"))
