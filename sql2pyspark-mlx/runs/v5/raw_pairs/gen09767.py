from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") >= "2023-07-01") & (F.col("event_time") < "2023-12-01") & F.col("page").isin(["/home", "/product"]))
    .orderBy(F.col("event_time").desc(), "id")
    .limit(50)
)
