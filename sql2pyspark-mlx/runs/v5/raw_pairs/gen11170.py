from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~((F.col("page") == "/home") & (F.col("duration_sec") <= 205)))
    .groupBy("page")
    .agg(F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"))
)
