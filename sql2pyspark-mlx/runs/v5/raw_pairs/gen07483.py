from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .select(F.col("we.page"), F.col("s.country"), F.col("s.customer_id"))
    .orderBy("customer_id")
)
