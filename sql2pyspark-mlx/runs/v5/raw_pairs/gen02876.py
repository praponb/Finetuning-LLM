from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec_2"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
    )
    .orderBy("duration_sec_bit_and")
)
