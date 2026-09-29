from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.col("session_id").isin([12, 14, 18, 32])).alias("we"), F.col("s.id") == F.col("we.session_id"), "left_semi")
    .filter(~F.col("s.browser").isin(["Chrome", "Safari", "Edge"]))
    .select(F.col("s.id"), F.col("s.country"))
    .orderBy(F.col("id").desc())
)
