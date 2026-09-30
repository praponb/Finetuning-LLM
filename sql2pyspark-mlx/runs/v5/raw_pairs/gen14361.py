from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("page") == "/checkout") & (F.col("duration_sec") != 336))
    .agg(
        F.countDistinct("duration_sec").alias("unique_pages"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
    )
)
