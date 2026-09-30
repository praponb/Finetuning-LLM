from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "event_type").orderBy("duration_sec", "id")

ranked = (
    df_web_events
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .withColumn("activity_month", F.date_trunc("MONTH", "event_time"))
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 461, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["activity_month", "duration_sec_tier"], ["activity_month"], []],
        "activity_month",
        "duration_sec_tier",
    )
    .agg(
        F.max("event_time").alias("last_event_time"),
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        F.count("*").alias("row_count"),
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
    )
)
