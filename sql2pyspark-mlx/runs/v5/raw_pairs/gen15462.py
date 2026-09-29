from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")

result = (
    df_web_events
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 1)
    .select("id", "event_type", "event_time", "page")
)
