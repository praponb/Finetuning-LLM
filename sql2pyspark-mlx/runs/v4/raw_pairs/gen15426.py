from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc())

result = (
    df_web_events
    .filter(F.col("event_type") == "add_to_cart")
    .select("id", "event_type", "duration_sec", F.rank().over(w).alias("rnk"))
)
