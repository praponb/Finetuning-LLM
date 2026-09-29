from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("session_id") == 32) & (F.col("page") == "/product") & F.col("session_id").isin([16, 18, 32]))
    .groupBy(
        F.when(F.col("duration_sec") >= 434, "high").when(F.col("duration_sec") >= 142, "mid").otherwise("low").alias("duration_sec_bucket"),
        "event_type",
        "page",
    )
    .agg(
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_count"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias("click_count"),
    )
)
