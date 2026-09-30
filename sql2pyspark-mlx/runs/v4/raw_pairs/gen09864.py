from pyspark.sql import functions as F

recent_web_events = df_web_events.filter((F.col("session_id") == 21) & F.col("page").isNotNull())

summary = (
    recent_web_events
    .groupBy("page")
    .agg(
        F.sum(F.when(F.col("page") == "/search", F.col("duration_sec")).otherwise(0)).alias("search_duration_sec"),
    )
)

result = (
    summary
    .filter(F.col("search_duration_sec") > 27)
    .select("page", "search_duration_sec")
)
