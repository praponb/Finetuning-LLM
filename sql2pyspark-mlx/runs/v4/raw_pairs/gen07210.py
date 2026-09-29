from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_duration_sec").desc())

result = (
    df_web_events
    .groupBy("page")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.count("*").alias("cnt"),
    )
    .withColumn("duration_sec_rank", F.rank().over(w))
)
