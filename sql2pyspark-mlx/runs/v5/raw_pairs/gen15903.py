from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") > 365) & (F.col("duration_sec") <= 449))
    .groupBy("page")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
    )
    .filter(F.col("stddev_duration_sec") >= 31)
    .orderBy("stddev_duration_sec")
)
