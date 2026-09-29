from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("event_time").desc(), "id")

result = (
    df_web_events
    .filter(F.col("event_type").isin(["purchase", "click", "add_to_cart"]))
    .select("id", "event_type", "event_time", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") == 1)
    .select("id", "event_type", "event_time")
    .orderBy("event_type")
)
