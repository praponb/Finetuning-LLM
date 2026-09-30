from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(((F.col("event_type") != "page_view") & F.col("session_id").isin([9, 10, 13, 14])) | (F.col("event_time") < "2024-09-15"))
    .groupBy("page")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("page") == "/home", 1).otherwise(0))) / F.count("*")), 1).alias("pct_home"),
    )
    .orderBy("page")
    .limit(5)
)
