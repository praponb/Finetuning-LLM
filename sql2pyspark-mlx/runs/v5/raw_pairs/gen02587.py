from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("status_code").desc())
w2 = Window.orderBy(F.col("is_debug").desc_nulls_first())
w3 = Window.partitionBy("level")
w4 = Window.partitionBy("k").orderBy("is_debug")

lines = (
    df_app_logs
    .select("id", "level", "status_code", F.explode_outer("context").alias("k", "v"))
)

base = (
    lines
    .filter(F.col("k").isNotNull())
    .select(
        "*",
        F.substring("level", 1, 2).alias("level_prefix"),
        F.col("status_code").cast("double").alias("status_code_num"),
        F.coalesce("k", F.lit("N/A")).alias("k_filled"),
    )
)

valid = (
    base
    .filter(F.col("k").isNotNull() & ~F.col("k").isin(["trace", "region"]))
    .select("*", F.when(F.col("level") == "DEBUG", 1).otherwise(0).alias("is_debug"))
)

cleaned = (
    valid
    .filter(~F.col("k").eqNullSafe(F.lit("version")))
    .withColumn("status_code_num_adjusted", F.round((F.col("status_code_num") * 100), 2))
)

filtered = (
    cleaned
    .filter(F.col("level") == "DEBUG")
    .select(
        "id",
        "level",
        "status_code",
        "k",
        "status_code_num",
        "k_filled",
        "is_debug",
        "status_code_num_adjusted",
        (F.col("status_code_num_adjusted") - F.col("status_code_num")).alias("status_code_num_adjusted_minus_status_code_num"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "level",
        "k",
        "k_filled",
        "is_debug",
        "status_code_num_adjusted",
        F.dense_rank().over(w).alias("status_code_dense_rank"),
        F.cume_dist().over(w2).alias("is_debug_cume_dist"),
        F.count("status_code_num_adjusted").over(w3).alias("level_count_status_code_num_adjusted"),
        F.cume_dist().over(w4).alias("is_debug_cume_dist_2"),
    )
)

result = (
    metrics
    .select(
        "id",
        "level",
        "k",
        "k_filled",
        "is_debug",
        "status_code_num_adjusted",
        "status_code_dense_rank",
        "is_debug_cume_dist",
        "level_count_status_code_num_adjusted",
    )
    .orderBy(F.col("id").desc())
)
