from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "click")
    .groupBy("page")
    .agg(F.count("*").alias("row_count"))
    .filter(F.col("row_count") < 5)
    .select("page", "row_count")
)
