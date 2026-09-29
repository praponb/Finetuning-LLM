from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.to_date("event_time") >= "2023-06-01") & F.col("event_type").isin(["page_view", "search", "add_to_cart"]))
    .groupBy(
        F.when(F.col("duration_sec") >= 431, "high").when(F.col("duration_sec") >= 71, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.round(((100 * F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0))) / F.count("*")), 1).alias("pct_page_view"),
    )
    .filter(F.col("pct_page_view") >= 65)
    .orderBy(F.col("pct_page_view").desc(), "duration_sec_bucket")
    .limit(50)
)
