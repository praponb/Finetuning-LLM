from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.col("duration_sec") >= 356).alias("we"), F.col("s.id") == F.col("we.session_id"), "left_semi")
    .select(F.col("s.id"), F.col("s.customer_id"))
    .orderBy(F.col("id").desc(), F.col("customer_id").desc())
)
