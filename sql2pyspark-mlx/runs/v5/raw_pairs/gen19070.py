from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.device").isin(["desktop", "mobile", "tablet"]) & (F.col("we.page") == "/product"))
    .select(
        F.col("s.customer_id"),
        F.col("s.started_at"),
        F.col("s.country"),
        F.col("we.page"),
    )
)
