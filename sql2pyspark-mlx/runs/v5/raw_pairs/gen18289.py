from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.col("event_time") < "2023-04-21").alias("we"), F.col("s.id") == F.col("we.session_id"), "left_semi")
    .filter(F.col("s.browser").isin(["Edge", "Safari", "Firefox"]))
    .select(F.col("s.id"), F.col("s.customer_id"))
)
