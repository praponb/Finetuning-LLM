from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("event_time").desc())

result = (
    df_web_events
    .filter(F.col("session_id").isin([8, 33, 38, 39]))
    .select("id", "page", "event_time", F.dense_rank().over(w).alias("dense_rnk"))
)
