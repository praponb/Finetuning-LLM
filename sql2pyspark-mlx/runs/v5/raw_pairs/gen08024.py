from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.countDistinct("page").alias("distinct_pages"),
        F.min("duration_sec").alias("min_duration_sec"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        F.max(F.when(F.col("page") == "/product", F.col("duration_sec"))).alias("product_max_duration_sec"),
    )
    .filter(F.col("avg_duration_sec") < 1000000)
    .orderBy(F.col("distinct_pages").desc_nulls_first())
)
