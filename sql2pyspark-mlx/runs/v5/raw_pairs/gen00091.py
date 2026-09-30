from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") < 458)
    .groupBy("page")
    .agg(
        F.count("duration_sec").alias("duration_sec_count"),
        F.countDistinct("duration_sec").alias("unique_event_types"),
    )
    .filter(F.col("unique_event_types") < 8)
    .orderBy("page")
)
