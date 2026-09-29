from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("page").isin(["/checkout", "/product"]))
    .select("id", "page", "event_time")
    .orderBy(F.col("event_time").desc(), F.col("page").desc())
)
