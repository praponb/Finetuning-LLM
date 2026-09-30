from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc(), "id")

result = (
    df_web_events
    .filter(F.col("duration_sec") >= 163)
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 3)
)
