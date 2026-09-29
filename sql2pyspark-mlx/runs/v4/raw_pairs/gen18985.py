from pyspark.sql import functions as F

result = (
    df_web_events
    .select("page", "event_time")
    .orderBy(F.col("page").desc(), "event_time")
)
