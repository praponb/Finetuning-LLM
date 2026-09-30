from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_duration_sec").desc())

result = (
    df_web_events
    .groupBy("page")
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
    .withColumn("rnk", F.rank().over(w))
)
