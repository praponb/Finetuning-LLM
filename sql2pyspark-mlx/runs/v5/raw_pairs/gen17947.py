from pyspark.sql import functions as F

result = (
    df_web_events
    .select("id", "event_type", "page", F.length("page").alias("page_length"))
)
