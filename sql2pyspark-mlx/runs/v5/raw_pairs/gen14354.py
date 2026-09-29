from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("status_code").desc())
w2 = Window.partitionBy("attr_key").orderBy(F.col("status_code").desc())
w3 = Window.partitionBy("level")
w4 = Window.orderBy(F.col("status_code_cume_dist").desc_nulls_first())
w5 = Window.partitionBy("service")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

base = (
    flattened
    .filter(~F.col("attr_key").eqNullSafe(F.lit("version")))
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        "attr_key",
        "attr_value",
        F.concat("attr_key", F.lit(", "), "host").alias("attr_key_host"),
    )
)

enriched = (
    base
    .select(
        "id",
        "service",
        "level",
        "status_code",
        "attr_value",
        F.percent_rank().over(w).alias("status_code_percent_rank"),
        F.cume_dist().over(w2).alias("status_code_cume_dist"),
    )
)

enriched_app_logs = (
    enriched
    .select(
        "*",
        F.stddev_samp("status_code_percent_rank").over(w3).alias("level_stddev_samp_status_code_percent_rank"),
        F.round((F.col("status_code") / F.sum("status_code").over(w3)), 4).alias("status_code_share"),
        F.dense_rank().over(w4).alias("status_code_cume_dist_dense_rank"),
        F.percent_rank().over(w).alias("status_code_percent_rank_2"),
        F.count("status_code_percent_rank").over(w5).alias("service_count_status_code_percent_rank"),
    )
)

filtered = (
    enriched_app_logs
    .filter(F.col("status_code") == 414)
    .select(
        "*",
        F.round("status_code_cume_dist", 0).alias("status_code_cume_dist_rounded"),
    )
)

result = (
    filtered
    .select(
        "id",
        "service",
        "level",
        "status_code",
        "attr_value",
        "status_code_percent_rank",
        "status_code_share",
        "status_code_cume_dist_dense_rank",
        "status_code_percent_rank_2",
    )
)
