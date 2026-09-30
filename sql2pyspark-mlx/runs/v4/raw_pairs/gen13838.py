from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type", "page")
    .agg(
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        F.max(F.when(F.col("page") == "/search", F.col("duration_sec"))).alias("search_max_duration_sec"),
        F.count_if(F.col("page") == "/product").alias("product_events"),
    )
    .orderBy(F.col("search_max_duration_sec").desc())
)
