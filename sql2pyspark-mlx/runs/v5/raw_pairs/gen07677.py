from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("page") == "/product") & F.col("event_type").isin(["page_view", "search", "add_to_cart"]))
    .groupBy(
        "page",
        F.when(F.col("duration_sec") >= 534, "high").when(F.col("duration_sec") >= 71, "mid").otherwise("low").alias("duration_sec_bucket"),
        F.date_format("event_time", "yyyy-MM").alias("event_month"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
        F.round(((100 * F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0))) / F.count("*")), 1).alias("pct_page_view"),
    )
)
