from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([7, 13, 21]) & (F.to_date("event_time") < "2023-09-01"))
    .withColumn("event_type_upper", F.upper("event_type"))
    .withColumn("page_event_type", F.concat("page", F.lit("/"), "event_type"))
)
