from pyspark.sql import functions as F

valid_web_events = df_web_events.filter(F.col("session_id").isin([3, 36]))

summary = (
    valid_web_events
    .groupBy("page")
    .agg(
        F.sum(F.when(F.col("page") == "/search", 1).otherwise(0)).alias("search_count"),
        F.count("*").alias("cnt"),
    )
)

result = (
    summary
    .select("page", "search_count", "cnt")
    .orderBy(F.col("search_count").desc(), "page")
)
