from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page", "session_id")
    .agg(F.count("*").alias("num_web_events"))
    .filter(F.col("num_web_events") >= 3)
    .orderBy(F.col("num_web_events").desc(), "page", "session_id")
    .limit(5)
)
