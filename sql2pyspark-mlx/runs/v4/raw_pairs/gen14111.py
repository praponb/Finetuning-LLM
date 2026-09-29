from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key")
w2 = Window.partitionBy("host", "attr_key")
w3 = Window.partitionBy("attr_key", "host")
w4 = Window.partitionBy("host")
w5 = Window.orderBy(F.col("latency_ms").asc_nulls_last())
w_grp = Window.partitionBy("attr_key", "host").orderBy(F.col("is_version").desc_nulls_first())
w7 = Window.orderBy(F.col("attr_key_count_is_version").asc_nulls_last())
w8 = Window.partitionBy("attr_key").orderBy(F.col("attr_key_count_is_version").desc_nulls_last())

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

valid = (
    flattened
    .filter(F.col("latency_ms") < 1661)
    .select(
        "*",
        F.when(F.col("attr_key") == "version", 1).otherwise(0).alias("is_version"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.corr("status_code", "latency_ms").over(w2).alias("corr_status_code_latency_ms"),
        F.count("is_version").over(w3).alias("attr_key_count_is_version"),
        F.min("latency_ms").over(w4).alias("host_min_latency_ms"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.percent_rank().over(w5).alias("latency_ms_percent_rank"),
        F.rank().over(w_grp).alias("is_version_rank"),
        F.rank().over(w7).alias("attr_key_count_is_version_rank"),
        F.percent_rank().over(w8).alias("attr_key_count_is_version_percent_rank"),
        F.max("is_version").over(w).alias("attr_key_max_is_version"),
    )
)

result = (
    enriched
    .select(
        "id",
        "status_code",
        "attr_key",
        "value",
        "is_version",
        "corr_status_code_latency_ms",
        "attr_key_count_is_version",
        "host_min_latency_ms",
        "is_version_rank",
    )
)
