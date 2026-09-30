from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.col("page") == "/product").alias("we"), F.col("s.id") == F.col("we.session_id"), "left_semi")
    .filter(F.hour(F.col("s.started_at")).between(10, 18))
    .select(F.col("s.id"), F.col("s.device"), F.col("s.customer_id"))
)
