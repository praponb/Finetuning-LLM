from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc(), "id")
w_recent = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(-2, -1)

cleaned = (
    df_web_events
    .filter(F.col("event_time") > F.lit("2024-05-13").cast("date"))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.col("duration_sec").cast("string").alias("duration_sec_num"),
        F.coalesce("page", F.lit("NONE")).alias("page_clean"),
    )
)

result = (
    cleaned
    .withColumn("duration_sec_bucket", F.ntile(3).over(w))
    .withColumn("duration_sec_avg_2_prior", F.avg("duration_sec").over(w_recent))
)
