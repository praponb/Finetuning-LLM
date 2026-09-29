from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("event_time").desc(), "id")

result = (
    df_web_events
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 5)
    .select("id", "page", "event_time", "event_type")
    .orderBy("page")
)
