from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("we.event_type").isin(["search", "add_to_cart"]))
    .select(
        F.col("we.page"),
        F.col("we.event_time"),
        F.col("we.session_id"),
        F.col("s.device"),
        F.col("s.customer_id"),
        F.length(F.col("we.event_type")).alias("event_type_length"),
    )
    .orderBy(F.col("event_type_length").desc())
)
