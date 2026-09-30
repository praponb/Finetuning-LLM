from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.col("event_type").isin(["search", "page_view", "click"])).alias("we"), F.col("s.id") == F.col("we.session_id"), "left_anti")
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("s.browser"),
        F.col("s.country"),
    )
    .orderBy("country", "id")
    .limit(10)
)
