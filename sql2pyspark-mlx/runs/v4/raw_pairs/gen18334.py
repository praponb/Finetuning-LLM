from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~((F.col("duration_sec") <= 429) & F.col("page").isin(["/home", "/product"])))
    .select("id", "page", "event_type", "duration_sec")
)
