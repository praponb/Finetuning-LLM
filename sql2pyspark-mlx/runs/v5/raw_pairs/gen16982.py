from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("page").isin(["/cart", "/search", "/home"]) & (F.col("event_type") != "purchase") & (F.to_date("event_time") < "2023-06-01"))
    .select("id", "event_time", F.lower("page").alias("page_lower"))
)
