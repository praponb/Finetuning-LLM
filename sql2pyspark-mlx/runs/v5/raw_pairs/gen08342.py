from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") != "/product")
    .groupBy("page")
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
    .filter(F.col("avg_duration_sec") < 420)
    .select("page", "avg_duration_sec")
    .orderBy("avg_duration_sec")
)
