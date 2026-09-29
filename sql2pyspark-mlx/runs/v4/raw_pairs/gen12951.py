from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc_nulls_last())
w2 = Window.partitionBy("page")

metrics = (
    df_web_events
    .withColumn("duration_sec_percent_rank", F.percent_rank().over(w))
    .withColumn("page_count_duration_sec", F.count("duration_sec").over(w2))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("duration_sec") >= 484, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.max("event_time").alias("ended_at"),
    )
    .orderBy(F.col("duration_sec_stddev").desc())
)
