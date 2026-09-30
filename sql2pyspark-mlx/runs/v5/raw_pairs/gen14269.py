from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("event_time").desc())

result = (
    df_web_events
    .filter(~F.col("page").isin(["/search", "/cart"]))
    .select("id", "event_type", "event_time", "page", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .select("id", "event_type", "event_time", "page")
    .orderBy("event_time", F.col("page").desc())
)
