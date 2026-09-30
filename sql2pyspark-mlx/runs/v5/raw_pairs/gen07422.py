from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("event_time").desc(), "id")

latest = (
    df_web_events
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .groupBy(
        F.when(F.col("duration_sec") >= 475, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
    )
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
    )
    .orderBy("num_event_types")
)
