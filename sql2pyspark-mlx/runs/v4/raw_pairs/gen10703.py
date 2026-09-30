from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.col("session_id") == 19).alias("we"), F.col("s.id") == F.col("we.session_id"), "left_anti")
    .filter(F.col("s.browser").isin(["Chrome", "Firefox"]))
    .select(F.col("s.id"), F.col("s.customer_id"), F.col("s.started_at"))
    .orderBy("customer_id", "id")
    .limit(10)
)
