from pyspark.sql import functions as F

avg_duration_sec = df_web_events.filter(F.col("event_type").isin(["page_view", "search", "purchase"])).agg(F.avg("duration_sec")).first()[0]

result = (
    df_web_events
    .filter(F.col("duration_sec") > avg_duration_sec)
    .select("id", "duration_sec")
)
