from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("we.page").isin(["/search", "/cart", "/checkout"]))
    .select(
        F.col("s.device"),
        F.col("s.started_at"),
        F.col("s.country"),
        F.col("we.session_id"),
    )
)
