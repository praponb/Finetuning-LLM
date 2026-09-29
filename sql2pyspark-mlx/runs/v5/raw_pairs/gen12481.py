from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("page").isin(["/checkout", "/home"]))
    .select("duration_sec", F.lower("event_type").alias("event_type_lower"))
    .orderBy("event_type_lower")
)
