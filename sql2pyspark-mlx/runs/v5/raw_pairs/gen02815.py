from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("duration_sec", "id")

result = (
    df_web_events
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 3)
    .orderBy(F.col("pos").desc())
)
