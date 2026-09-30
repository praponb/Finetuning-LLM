from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_duration_sec").desc())

result = (
    df_web_events
    .groupBy("event_type")
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
    .withColumn("rnk", F.dense_rank().over(w))
    .orderBy(F.col("total_duration_sec").desc(), F.col("rnk").desc())
)
