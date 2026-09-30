from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter((F.to_date(F.col("we.event_time")) >= "2023-09-01") & F.col("s.country").isin(["TH", "UK", "SG"]))
    .select(F.col("we.duration_sec"), F.col("we.event_time"), F.col("s.customer_id"))
)
