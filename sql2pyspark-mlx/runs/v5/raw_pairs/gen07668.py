from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy(F.col("latency_ms").desc())
w2 = Window.partitionBy("level")
w3 = Window.partitionBy("level", "key").orderBy("latency_ms")
w4 = Window.partitionBy("key").orderBy(F.col("latency_ms_share").desc())
w5 = Window.orderBy("latency_ms_percent_rank")
w6 = Window.partitionBy("key").orderBy(F.col("latency_ms_percent_rank_2").asc_nulls_last())
w7 = Window.partitionBy("host", "key")
w8 = Window.partitionBy("level").orderBy(F.col("key_length").desc())
w9 = Window.partitionBy("key", "host").orderBy(F.col("key_length").desc_nulls_first())

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

with_windows = (
    flattened
    .withColumn("latency_ms_percent_rank", F.percent_rank().over(w))
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4))
    .withColumn("latency_ms_percent_rank_2", F.percent_rank().over(w3))
)

scored = (
    with_windows
    .filter(F.col("key") != "version")
    .select(
        "*",
        F.cume_dist().over(w4).alias("latency_ms_share_cume_dist"),
        F.rank().over(w5).alias("latency_ms_percent_rank_rank"),
        F.dense_rank().over(w6).alias("latency_ms_percent_rank_2_dense_rank"),
    )
)

cleaned = (
    scored
    .filter(F.col("latency_ms_percent_rank_2_dense_rank") >= 5)
    .withColumn("key_length", F.length("key"))
    .withColumn("level_upper", F.upper("level"))
    .withColumn("resolved_key", F.coalesce("key", F.lit("OTHER")))
)

cleaned_app_logs = (
    cleaned
    .filter(F.col("key").isNotNull())
    .select(
        "*",
        F.when(F.col("host").isin(["h4", "h3"]), "Core").otherwise("Other").alias("host_group"),
        F.coalesce("key", F.lit("N/A")).alias("resolved_key_2"),
    )
)

metrics = (
    cleaned_app_logs
    .select(
        "host",
        "level",
        "latency_ms",
        "key",
        "attr_value",
        "latency_ms_share_cume_dist",
        "latency_ms_percent_rank_rank",
        "resolved_key",
        F.stddev_samp("latency_ms_percent_rank_2_dense_rank").over(w7).alias("host_stddev_samp_latency_ms_percent_rank_2_dense_rank"),
        F.cume_dist().over(w8).alias("key_length_cume_dist"),
        F.dense_rank().over(w9).alias("key_length_dense_rank"),
    )
)

result = (
    metrics
    .select(
        "level",
        F.col("key_length_cume_dist").alias("KEY_LENGTH_CUME_DIST_VAL"),
        "latency_ms_share_cume_dist",
    )
    .unpivot(
        ids=["level"],
        values=["KEY_LENGTH_CUME_DIST_VAL", "latency_ms_share_cume_dist"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
)
