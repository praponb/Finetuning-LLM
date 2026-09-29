from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(~((F.col("we.event_time") >= "2024-10-05") & ~F.col("we.page").isin(["/product", "/search"])))
    .select(
        F.col("we.id"),
        F.col("we.page"),
        F.col("we.session_id"),
        F.col("s.customer_id"),
        F.col("s.device"),
        F.col("s.browser"),
    )
)
