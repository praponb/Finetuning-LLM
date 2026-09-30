from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_duration_sec").desc())

result = (
    df_web_events
    .filter(F.hour("event_time").between(7, 11))
    .groupBy(
        F.when(F.col("duration_sec") >= 528, "high").when(F.col("duration_sec") >= 208, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
    .withColumn("duration_sec_rank", F.rank().over(w))
    .orderBy("duration_sec_rank")
)
