from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 530, "high").when(F.col("duration_sec") >= 61, "mid").otherwise("low").alias("duration_sec_bucket"),
        "page",
        F.year("event_time").alias("event_year"),
    )
    .agg(
        F.count(F.when(F.col("event_type") == "click", 1)).alias("click_count"),
        F.round(((100 * F.sum(F.when(F.col("page") == "/home", 1).otherwise(0))) / F.count("*")), 1).alias("pct_home"),
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
    )
)
