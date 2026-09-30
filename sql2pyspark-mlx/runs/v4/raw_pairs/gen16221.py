from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("event_cohort")

result = (
    df_web_events
    .groupBy(
        F.when(F.year("event_time") < 2024, "before_2024").when(F.year("event_time") == 2024, "2024").otherwise("after_2024").alias("event_cohort"),
    )
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_duration_sec", F.sum("total_duration_sec").over(w))
    .orderBy("event_cohort", F.col("total_duration_sec").desc())
)
