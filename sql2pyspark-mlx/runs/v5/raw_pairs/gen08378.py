from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") == 461) & (F.col("duration_sec") < 131))
    .orderBy("session_id", "id")
    .limit(5)
)
