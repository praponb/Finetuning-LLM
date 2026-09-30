from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("event_time").desc())

result = (
    df_web_events
    .filter(F.col("session_id").isin([32, 33]))
    .select("id", "event_type", "event_time", F.dense_rank().over(w).alias("dense_rnk"))
    .orderBy(F.col("event_time").desc(), F.col("event_type").desc())
)
