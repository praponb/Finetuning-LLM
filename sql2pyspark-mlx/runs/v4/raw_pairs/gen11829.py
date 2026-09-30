from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("we.event_type").isin(["page_view", "purchase", "click"]) | (F.col("we.page") == "/search"))
    .select(
        F.col("we.id"),
        F.col("we.event_type"),
        F.col("we.page"),
        F.col("s.customer_id"),
        F.col("s.browser"),
        F.col("s.country"),
        F.lower(F.col("we.event_type")).alias("event_type_lower"),
    )
)
