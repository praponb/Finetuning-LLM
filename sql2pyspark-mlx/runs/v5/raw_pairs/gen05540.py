from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time")

deduped = (
    df_web_events
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "session_id", "page", "duration_sec")
)

result = (
    deduped
    .groupBy("session_id")
    .agg(
        F.count(F.when(F.col("page") == "/home", 1)).alias("home_count"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.count(F.when(F.col("page") == "/search", 1)).alias("search_rows"),
        F.count(F.when(F.col("page") == "/search", 1)).alias("search_rows_2"),
        F.count(F.when(F.col("page") == "/home", 1)).alias("home_rows"),
    )
)
