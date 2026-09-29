from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("is_region").asc_nulls_last())
w2 = Window.partitionBy("level", "host")
w3 = Window.partitionBy("k")
w4 = Window.orderBy(F.col("is_region").desc_nulls_last())
w_ordered = Window.partitionBy("k").orderBy(F.col("k_stddev_samp_is_region").desc_nulls_last())
w6 = Window.partitionBy("host", "k").orderBy("is_region_cume_dist")
w7 = Window.partitionBy("host", "level")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

valid = (
    lines
    .filter(F.col("k").isNotNull())
    .withColumn("is_region", F.when(F.col("k") == "region", 1).otherwise(0))
    .withColumn("k_lower", F.lower("k"))
)

metrics = (
    valid
    .withColumn("is_region_cume_dist", F.cume_dist().over(w))
    .withColumn("level_stddev_samp_is_region", F.stddev_samp("is_region").over(w2))
    .withColumn("k_stddev_samp_is_region", F.stddev_samp("is_region").over(w3))
    .withColumn("is_region_percent_rank", F.percent_rank().over(w4))
)

base = (
    metrics
    .filter(F.col("k").isNotNull())
    .select(
        "id",
        "host",
        "level",
        "k",
        "attr_value",
        "is_region",
        "k_lower",
        "is_region_cume_dist",
        "level_stddev_samp_is_region",
        "k_stddev_samp_is_region",
        "is_region_percent_rank",
        F.least("k_stddev_samp_is_region", "level_stddev_samp_is_region").alias("least_k_stddev_samp_is_region_level_stddev_samp_is_region"),
        F.coalesce("k", F.lit("DIRECT")).alias("resolved_k"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.dense_rank().over(w_ordered).alias("k_stddev_samp_is_region_dense_rank"),
        F.percent_rank().over(w6).alias("is_region_cume_dist_percent_rank"),
        F.min("is_region").over(w7).alias("host_min_is_region"),
    )
)

base_app_logs = (
    with_windows
    .filter(F.col("k").isNotNull() & F.col("level_stddev_samp_is_region").isNotNull())
    .select(
        "*",
        (F.col("is_region_cume_dist") + F.col("k_stddev_samp_is_region_dense_rank")).alias("is_region_cume_dist_plus_k_stddev_samp_is_region_dense_rank"),
        F.coalesce("k", F.lit("NONE")).alias("resolved_k_2"),
    )
)

remaining_keys = (
    base_app_logs
    .select("level", "host")
    .subtract(
        df_app_logs
        .filter(F.col("status_code") < 276)
        .select("level", "host")
    )
)

verified = (
    base_app_logs
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.host") == F.col("k.host")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("level")
    .pivot("k", ["trace", "region"])
    .agg(F.max("is_region_cume_dist_percent_rank"))
)
