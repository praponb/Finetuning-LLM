from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "service").orderBy(F.col("latency_ms").desc_nulls_first())
w2 = Window.partitionBy("level", "service")
w3 = Window.partitionBy("attr_key").orderBy("status_code")
w4 = Window.orderBy(F.col("least_latency_ms_cume_dist_level_stddev_samp_status_code").desc_nulls_first())
w5 = Window.orderBy(F.col("latency_ms").asc_nulls_first())
w_ordered = Window.partitionBy("level")
w7 = Window.partitionBy("level").orderBy(F.col("status_code_percent_rank").asc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

cleaned = (
    flattened
    .filter(F.col("host").isin(["h1", "h4"]))
    .select(
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        "attr_key",
        "v",
        F.when(F.col("level").isin(["ERROR", "INFO"]), "Core").otherwise("Other").alias("level_group"),
        F.coalesce("attr_key", F.lit("UNKNOWN")).alias("resolved_attr_key"),
    )
)

enriched = (
    cleaned
    .select(
        "host",
        "level",
        "latency_ms",
        "level_group",
        F.cume_dist().over(w).alias("latency_ms_cume_dist"),
        F.stddev_samp("status_code").over(w2).alias("level_stddev_samp_status_code"),
        F.percent_rank().over(w3).alias("status_code_percent_rank"),
    )
)

base = (
    enriched
    .filter(F.col("latency_ms") == 2136)
    .select(
        "level",
        "latency_ms",
        "level_group",
        "latency_ms_cume_dist",
        "level_stddev_samp_status_code",
        "status_code_percent_rank",
        F.least("latency_ms_cume_dist", "level_stddev_samp_status_code").alias("least_latency_ms_cume_dist_level_stddev_samp_status_code"),
        F.upper("host").alias("host_upper"),
    )
)

scored = (
    base
    .select(
        "level",
        "latency_ms",
        "latency_ms_cume_dist",
        "level_stddev_samp_status_code",
        "status_code_percent_rank",
        "least_latency_ms_cume_dist_level_stddev_samp_status_code",
        "host_upper",
        F.cume_dist().over(w4).alias("least_latency_ms_cume_dist_level_stddev_samp_status_code_cume_dist"),
        F.percent_rank().over(w5).alias("latency_ms_percent_rank"),
        F.stddev_samp("least_latency_ms_cume_dist_level_stddev_samp_status_code").over(w_ordered).alias("level_stddev_samp_least_latency_ms_cume_dist_level_stddev_samp_status_code"),
        F.cume_dist().over(w7).alias("status_code_percent_rank_cume_dist"),
    )
)

result = (
    scored
    .cube(
        F.when(F.col("status_code_percent_rank_cume_dist") >= 0.82, "HIGH").otherwise("LOW").alias("status_code_percent_rank_cume_dist_tier"),
        "level",
    )
    .agg(
        F.countDistinct("level").alias("distinct_levels"),
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
    )
)
