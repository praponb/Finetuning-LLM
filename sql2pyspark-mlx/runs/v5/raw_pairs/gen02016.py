from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter((F.col("we.page") == "/checkout") | F.col("s.country").isin(["JP", "UK", "US"]))
    .select(F.col("we.duration_sec"), F.col("we.session_id"), F.col("s.customer_id"))
)
