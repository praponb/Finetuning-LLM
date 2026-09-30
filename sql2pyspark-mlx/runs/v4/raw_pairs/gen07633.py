from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/cart")
    .select("id", "event_type", "duration_sec", "page")
    .orderBy(F.col("id").desc())
    .limit(5)
)
