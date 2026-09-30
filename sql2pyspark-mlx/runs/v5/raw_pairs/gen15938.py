from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([14, 31]))
    .withColumn("event_time_label", F.date_format("event_time", "MMM yyyy"))
    .withColumn("event_time_date", F.to_date("event_time"))
)
