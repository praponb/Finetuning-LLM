from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([18, 32]))
    .select("id", "session_id", "duration_sec")
)
