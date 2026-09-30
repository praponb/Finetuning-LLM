from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .agg(
        F.count("*").alias("web_event_count"),
        F.count(F.when(F.col("page") == "/search", 1)).alias("search_count"),
    )
    .filter(F.col("search_count") > 4)
    .select("page", "web_event_count", "search_count")
)
