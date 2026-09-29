from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/cart")
    .orderBy(F.col("page").desc(), "event_type")
)
