from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc(), "id")

valid = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        F.col("duration_sec").cast("double").alias("duration_sec_num"),
    )
)

latest = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

latest_web_events = (
    latest
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest_web_events
    .groupBy("session_id")
    .pivot("event_type", ["search", "click"])
    .agg(F.max("duration_sec_num").alias("total"), F.count(F.lit(1)).alias("cnt"))
)
