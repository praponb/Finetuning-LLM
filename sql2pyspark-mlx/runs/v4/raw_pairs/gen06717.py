from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_type_length", "id")

filtered = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-09-02 03:00:00").cast("timestamp")) & (F.col("event_time") >= "2024-06-01"))
    .select(
        "id",
        "session_id",
        "event_type",
        "duration_sec",
        F.length("event_type").alias("event_type_length"),
    )
)

result = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)
