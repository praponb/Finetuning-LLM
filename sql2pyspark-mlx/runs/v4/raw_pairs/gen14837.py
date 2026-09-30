from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.col("we.page").isin(["/home", "/cart", "/search"]))
    .groupBy(F.col("s.id"), F.col("s.country"))
    .agg(F.count(F.col("we.id")).alias("web_event_count"))
    .filter(F.col("web_event_count") > 5)
)
