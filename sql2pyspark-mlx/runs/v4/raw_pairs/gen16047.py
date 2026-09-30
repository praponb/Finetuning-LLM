from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("duration_sec_whole")
w2 = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("session_id", "event_type").orderBy("duration_sec_whole", "id").rowsBetween(-2, -1)

cleaned = (
    df_web_events
    .filter(F.col("session_id").isin([5, 11, 27, 28]))
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        F.ceil("duration_sec").alias("duration_sec_whole"),
    )
)

result = (
    cleaned
    .withColumn("duration_sec_whole_percent_rank", F.percent_rank().over(w_recent))
    .withColumn("running_duration_sec", F.sum("duration_sec").over(w2))
    .withColumn("duration_sec_whole_avg_2_prior", F.avg("duration_sec_whole").over(w_ordered))
)
