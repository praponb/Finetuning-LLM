from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["search", "purchase", "page_view"]))
    .select("event_type", "page")
    .distinct()
)
