from pyspark.sql import functions as F

valid_web_events = (
    df_web_events
    .filter(F.col("page").isin(["/product", "/checkout"]) & F.col("session_id").isin([8, 16, 24]) & F.col("page").isin(["/product", "/search", "/home"]))
)

summary = (
    valid_web_events
    .groupBy(
        F.when(F.col("page").isin(["/home", "/product"]), "priority").otherwise("other").alias("page_group"),
        "session_id",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max("event_time").alias("last_event_time"),
    )
)

result = summary.orderBy("session_id")
