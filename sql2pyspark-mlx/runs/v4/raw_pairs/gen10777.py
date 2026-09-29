from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") != "/checkout")
    .withColumn("event_time_period", F.date_trunc("year", "event_time"))
    .orderBy("duration_sec", "id")
    .limit(10)
)
