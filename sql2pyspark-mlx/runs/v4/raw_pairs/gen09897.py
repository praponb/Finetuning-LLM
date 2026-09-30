from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("page") == "/cart") & (F.col("event_type") == "click") & F.col("event_type").isin(["page_view", "search"]))
    .select("event_type", "session_id")
)
