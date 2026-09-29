from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") < "2025-05-18") & (F.col("session_id").isin([8, 19]) | (F.col("duration_sec") != 444)))
    .withColumn("event_type_upper", F.upper("event_type"))
)
