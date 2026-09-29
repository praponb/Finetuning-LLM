from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("is_h1").desc())
w2 = Window.orderBy(F.col("is_h1").desc_nulls_last())
w3 = Window.partitionBy("level")
w4 = Window.orderBy(F.col("is_h1").asc_nulls_first())

lines = (
    df_app_logs
    .select("id", "host", "level", F.explode_outer("context").alias("key", "v"))
)

filtered = (
    lines
    .filter(F.col("key").isNotNull() & F.col("key").isin(["version", "region", "trace"]))
    .select(
        "id",
        "host",
        "level",
        "key",
        "v",
        F.when(F.col("host") == "h1", 1).otherwise(0).alias("is_h1"),
        F.concat("key", F.lit(" "), "host").alias("key_host"),
        F.coalesce("key", F.lit("N/A")).alias("resolved_key"),
    )
)

filtered_app_logs = (
    filtered
    .filter(F.col("key").eqNullSafe(F.lit("region")))
    .select(
        "id",
        "host",
        "level",
        "v",
        "is_h1",
        "key_host",
        F.round((F.col("is_h1") * 1.07), 2).alias("is_h1_adjusted"),
    )
)

metrics = (
    filtered_app_logs
    .select(
        "*",
        F.dense_rank().over(w).alias("is_h1_dense_rank"),
        F.dense_rank().over(w2).alias("is_h1_dense_rank_2"),
        F.min("is_h1").over(w3).alias("level_min_is_h1"),
        F.max("is_h1_adjusted").over(w3).alias("level_max_is_h1_adjusted"),
        F.rank().over(w4).alias("is_h1_rank"),
    )
)

result = (
    metrics
    .rollup("host", "level")
    .agg(
        F.countDistinct("level").alias("distinct_levels"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.percentile_approx("is_h1_rank", [0.25, 0.5, 0.75]).alias("is_h1_rank_quartiles"),
        F.round(F.sum("level_min_is_h1") / F.nullif(F.count(F.when(F.col("level") == "DEBUG", 1)), F.lit(0)), 2).alias("level_min_is_h1_per_debug"),
        F.grouping("host").alias("gid"),
    )
    .filter(F.col("level_min_is_h1_per_debug") < 1000000)
    .orderBy(F.col("host").desc())
)
