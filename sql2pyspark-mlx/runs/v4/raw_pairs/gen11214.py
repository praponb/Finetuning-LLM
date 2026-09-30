from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/product")
    .groupBy("page")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("duration_sec").alias("unique_event_types"),
    )
    .filter(F.col("row_count") < 1)
)
