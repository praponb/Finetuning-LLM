from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
    )
    .filter(F.col("avg_duration_sec") > 64)
)
