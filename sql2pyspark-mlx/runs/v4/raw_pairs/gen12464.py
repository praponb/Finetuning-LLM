from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["search", "purchase", "page_view"]))
    .groupBy("page")
    .agg(F.min("duration_sec").alias("min_duration_sec"))
)
