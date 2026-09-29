from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(243, 361) & (F.to_date("event_time") >= "2024-07-01"))
)
