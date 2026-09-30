from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.country") == "TH")
    .select(F.col("s.browser"), F.col("s.customer_id"), F.col("we.session_id"))
    .orderBy("browser")
)
