from pyspark.sql import functions as F

src = (
    df_web_events
    .filter(F.col("page").isin(["/product", "/home", "/search"]) & (F.col("duration_sec") > 432))
)

summary = (
    src
    .groupBy("session_id")
    .agg(
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_count"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
)

result = (
    summary
    .filter(F.col("avg_duration_sec") < 75)
    .select("session_id", "avg_duration_sec")
    .orderBy("session_id")
)
