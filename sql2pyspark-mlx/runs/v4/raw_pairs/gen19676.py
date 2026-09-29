from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.countDistinct("duration_sec").alias("unique_pages"),
    )
    .filter(F.col("unique_pages") < 2)
    .orderBy("unique_pages")
)
