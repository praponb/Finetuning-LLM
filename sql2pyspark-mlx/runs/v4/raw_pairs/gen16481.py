from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("attr_key")
w_running = Window.partitionBy("level").orderBy(F.col("latency_ms").desc_nulls_first())
w3 = Window.partitionBy("attr_key", "level").orderBy("attr_key_stddev_samp_latency_ms")
w4 = Window.partitionBy("attr_key", "level").orderBy(F.col("latency_ms_percent_rank").desc())
w5 = Window.orderBy(F.col("greatest_latency_ms_percent_rank_int_latency_ms_percent_rank").desc_nulls_first())
w6 = Window.orderBy(F.col("attr_key_stddev_samp_latency_ms").asc_nulls_last())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

matched_keys = (
    lines
    .select("id", "service")
    .intersect(
        df_app_logs
        .filter(F.col("level") == "ERROR")
        .select("id", "service")
    )
)

kept_rows = (
    lines
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .withColumn("attr_key_stddev_samp_latency_ms", F.stddev_samp("latency_ms").over(w_part))
    .withColumn("latency_ms_percent_rank", F.percent_rank().over(w_running))
)

filtered = (
    enriched
    .filter(F.col("attr_key").isNotNull())
    .withColumn("latency_ms_percent_rank_int", F.col("latency_ms_percent_rank").cast("int"))
    .withColumn("clean_attr_key", F.trim("attr_key"))
)

base = (
    filtered
    .filter(F.col("attr_key").isNotNull() & (F.col("latency_ms_percent_rank") > 0.27))
    .select(
        "*",
        F.col("attr_key_stddev_samp_latency_ms").cast("string").alias("attr_key_stddev_samp_latency_ms_num"),
        F.substring("attr_key", 1, 2).alias("attr_key_prefix"),
    )
)

base_app_logs = (
    base
    .filter(F.col("attr_key") == "trace")
    .select(
        "id",
        "level",
        "latency_ms",
        "attr_key",
        "attr_value",
        "attr_key_stddev_samp_latency_ms",
        "latency_ms_percent_rank",
        "clean_attr_key",
        "attr_key_stddev_samp_latency_ms_num",
        "attr_key_prefix",
        F.greatest("latency_ms_percent_rank_int", "latency_ms_percent_rank").alias("greatest_latency_ms_percent_rank_int_latency_ms_percent_rank"),
        F.coalesce("attr_key", F.lit("UNKNOWN")).alias("resolved_attr_key"),
    )
)

with_windows = (
    base_app_logs
    .select(
        "*",
        F.cume_dist().over(w3).alias("attr_key_stddev_samp_latency_ms_cume_dist"),
        F.dense_rank().over(w4).alias("latency_ms_percent_rank_dense_rank"),
        F.rank().over(w5).alias("greatest_latency_ms_percent_rank_int_latency_ms_percent_rank_rank"),
        F.dense_rank().over(w6).alias("attr_key_stddev_samp_latency_ms_dense_rank"),
        F.min("latency_ms_percent_rank").over(w_part).alias("attr_key_min_latency_ms_percent_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "level",
        "latency_ms",
        "attr_value",
        "attr_key_stddev_samp_latency_ms",
        "latency_ms_percent_rank",
        "clean_attr_key",
        "attr_key_prefix",
        "attr_key_stddev_samp_latency_ms_cume_dist",
        F.when(F.col("level").rlike("^(INFO|WARN)$"), "EXTENDED").otherwise("PRIORITY").alias("level_zone"),
    )
    .orderBy(F.col("latency_ms").asc_nulls_first())
)
