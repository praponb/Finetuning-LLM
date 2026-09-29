from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter((F.col("s.device") != "mobile") & F.col("we.event_type").isin(["click", "search", "page_view"]))
    .select(
        F.col("s.browser"),
        F.col("we.event_type"),
        F.initcap(F.trim(F.col("we.page"))).alias("page_title"),
    )
)
