from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k", "host").orderBy(F.col("is_region").desc_nulls_first())
w2 = Window.partitionBy("host")
w3 = Window.partitionBy("k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w4 = Window.partitionBy("k").orderBy(F.col("is_region").desc_nulls_first())
w5 = Window.partitionBy("service", "host")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        F.explode_outer("context").alias("k", "v"),
    )
)

base = (
    exploded
    .filter(F.col("log_ts") >= F.to_timestamp(F.lit("2025-06-01 14:00:00")))
    .withColumn("is_region", F.when(F.col("k") == "region", 1).otherwise(0))
)

scored = (
    base
    .withColumn("is_region_dense_rank", F.dense_rank().over(w))
    .withColumn("host_stddev_samp_is_region", F.stddev_samp("is_region").over(w2))
    .withColumn("is_region_count_15m", F.count("is_region").over(w3))
    .withColumn("is_region_cume_dist", F.cume_dist().over(w4))
)

valid = (
    scored
    .filter((F.col("log_ts") > F.lit("2025-06-01 05:00:00").cast("timestamp")) & F.col("k").isNotNull())
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "v",
        "host_stddev_samp_is_region",
        "is_region_count_15m",
        "is_region_cume_dist",
        F.col("is_region_dense_rank").cast("string").alias("is_region_dense_rank_num"),
    )
)

valid_app_logs = (
    valid
    .filter(F.col("log_ts") >= F.lit("2025-06-01 07:00:00").cast("timestamp"))
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "v",
        "host_stddev_samp_is_region",
        "is_region_count_15m",
        "is_region_dense_rank_num",
        F.round("is_region_cume_dist", 1).alias("is_region_cume_dist_rounded"),
    )
)

enriched = (
    valid_app_logs
    .filter(~F.col("host").isin(["h2", "h1"]))
    .withColumn("is_region_count_15m_share", F.round((F.col("is_region_count_15m") / F.sum("is_region_count_15m").over(w5)), 4))
)

result = (
    enriched
    .select(
        "id",
        "log_ts",
        "host_stddev_samp_is_region",
        "is_region_count_15m",
        "is_region_dense_rank_num",
        "is_region_cume_dist_rounded",
        "is_region_count_15m_share",
    )
)
