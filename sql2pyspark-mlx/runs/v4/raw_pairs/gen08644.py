from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.col("duration_sec").between(193, 388)).alias("we"), F.col("s.id") == F.col("we.session_id"), "left_anti")
    .filter(F.to_date(F.col("s.started_at")) >= "2023-07-29")
    .select(F.col("s.id"), F.col("s.started_at"))
    .orderBy("started_at")
)
