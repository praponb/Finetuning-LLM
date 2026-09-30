from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "session_id").orderBy(F.col("event_time").desc_nulls_last(), "id")

deduped = (
    df_web_events
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "page", "event_time", "duration_sec")
)

result = (
    deduped
    .withColumn("quarter_start", F.date_trunc("quarter", "event_time"))
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 464, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["quarter_start", "page", "duration_sec_tier"], ["quarter_start", "page"], ["quarter_start"]],
        "quarter_start",
        "page",
        "duration_sec_tier",
    )
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.countDistinct("page").alias("distinct_pages"),
    )
    .filter(F.col("avg_duration_sec") < 1000000)
)
