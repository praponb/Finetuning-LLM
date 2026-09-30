from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "event_type").orderBy("event_time")

cleaned = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-09-19 14:00:00").cast("timestamp"))
    .select(
        "*",
        F.date_format("event_time", "yyyy-MM").alias("event_time_label"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("page_filled"),
    )
)

result = (
    cleaned
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)
