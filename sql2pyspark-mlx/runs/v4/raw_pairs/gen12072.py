from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.col("session_id").isin([8, 17, 23, 24]))
    .groupBy("session_id")
    .agg(F.count("*").alias("row_count"))
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .filter(F.col("ss.row_count") > 5)
    .select(F.col("s.customer_id"), F.col("ss.row_count"))
    .orderBy("row_count")
)
