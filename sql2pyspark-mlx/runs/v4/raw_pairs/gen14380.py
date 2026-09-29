from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "event_type").orderBy(F.col("duration_sec").desc_nulls_first(), "id")

deduped = (
    df_web_events
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 492, "HIGH").otherwise("LOW"))
    .cube("duration_sec_tier")
    .agg(
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.min("event_time").alias("first_seen"),
        F.grouping_id().alias("gid"),
    )
)
