from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k", "service").orderBy(F.col("k_length").desc())
w_part = Window.partitionBy("k", "service").orderBy(F.col("k_length").desc_nulls_last())
w_part_2 = Window.partitionBy("k", "service")
w4 = Window.partitionBy("host", "service")
w5 = Window.partitionBy("service", "k")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

cleaned = (
    exploded
    .filter(F.col("host") != "h3")
    .select(
        "service",
        "host",
        "k",
        F.concat("k", F.lit(", "), "service").alias("k_service"),
    )
)

filtered = (
    cleaned
    .filter(F.col("host").isin(["h3", "h1", "h2"]))
    .select("*", F.length("k").alias("k_length"), F.trim("host").alias("clean_host"))
)

with_windows = (
    filtered
    .withColumn("k_length_cume_dist", F.cume_dist().over(w))
    .withColumn("k_length_dense_rank", F.dense_rank().over(w_part))
    .withColumn("k_min_k_length", F.min("k_length").over(w_part_2))
    .withColumn("host_stddev_samp_k_length", F.stddev_samp("k_length").over(w4))
    .withColumn("service_count_k_length", F.count("k_length").over(w5))
)

daily = (
    with_windows
    .groupBy(
        F.when(F.col("k_length_dense_rank") >= 9, "LARGE").otherwise("SMALL").alias("k_length_dense_rank_tier"),
    )
    .agg(
        F.avg(F.when(F.col("service") == "search", F.col("k_length_cume_dist"))).alias("search_avg_k_length_cume_dist"),
        F.countDistinct("host").alias("distinct_hosts"),
        F.count(F.when(F.col("k") == "version", 1)).alias("version_rows"),
        F.count_if(F.col("host") == "h1").alias("h1_events"),
        F.sum(F.when(F.col("service") == "checkout", F.col("k_length_dense_rank")).otherwise(0)).alias("checkout_k_length_dense_rank"),
        F.count(F.when(F.col("k") == "region", 1)).alias("region_rows"),
    )
    .filter(F.col("region_rows") > 1)
)

result = (
    daily
    .select(
        "k_length_dense_rank_tier",
        "distinct_hosts",
        "version_rows",
        "h1_events",
        "checkout_k_length_dense_rank",
    )
)
