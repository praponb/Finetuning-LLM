from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["add_to_cart", "page_view"]))
    .select("duration_sec", F.upper("event_type").alias("event_type_upper"))
)
