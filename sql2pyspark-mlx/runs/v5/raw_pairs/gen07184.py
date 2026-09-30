from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.col("session_id").isin([5, 13, 16])).alias("we"), F.col("s.id") == F.col("we.session_id"), "left_semi")
    .filter(F.col("s.browser").isin(["Chrome", "Edge", "Safari"]))
    .select(F.col("s.id"), F.col("s.country"))
)
