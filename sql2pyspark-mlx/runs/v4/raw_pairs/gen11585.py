from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.orderBy(F.col("latency_ms").desc())
w_part = Window.partitionBy("service").orderBy("latency_ms")
w_hist = Window.partitionBy("service").orderBy(F.col("latency_ms").desc())
w5 = Window.partitionBy("service").orderBy(F.col("latency_ms_share_int").asc_nulls_first())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

scored = (
    exploded
    .select(
        "*",
        F.avg("latency_ms").over(w).alias("service_avg_latency_ms"),
        F.rank().over(w2).alias("latency_ms_rank"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4).alias("latency_ms_share"),
    )
)

base = (
    scored
    .filter(F.col("attr_key").isNotNull())
    .select(
        "id",
        "service",
        "latency_ms",
        "attr_value",
        "service_avg_latency_ms",
        "latency_ms_rank",
        "latency_ms_share",
        F.col("latency_ms_share").cast("int").alias("latency_ms_share_int"),
        F.when(F.col("attr_key") == "version", 1).otherwise(0).alias("is_version"),
    )
)

metrics = (
    base
    .withColumn("service_sum_latency_ms_share", F.sum("latency_ms_share").over(w))
    .withColumn("latency_ms_rank_2", F.rank().over(w_part))
    .withColumn("latency_ms_dense_rank", F.dense_rank().over(w_hist))
    .withColumn("latency_ms_share_int_rank", F.rank().over(w5))
    .withColumn("service_min_latency_ms_share", F.min("latency_ms_share").over(w))
)

valid = (
    metrics
    .filter(F.col("latency_ms_dense_rank") <= 3)
    .select(
        "latency_ms",
        "attr_value",
        "service_avg_latency_ms",
        "latency_ms_rank",
        "latency_ms_share",
        "is_version",
        "service_sum_latency_ms_share",
        "latency_ms_rank_2",
        "service_min_latency_ms_share",
        F.coalesce("latency_ms_rank_2", F.lit(0)).alias("latency_ms_rank_2_filled"),
        (F.col("latency_ms_share") * F.col("service_min_latency_ms_share")).alias("latency_ms_share_times_service_min_latency_ms_share"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("is_version") >= 71, "LARGE").otherwise("SMALL").alias("is_version_tier"),
        F.when(F.col("latency_ms_rank") >= 8, "LARGE").otherwise("SMALL").alias("latency_ms_rank_tier"),
    )
    .agg(
        F.round(F.avg("latency_ms_share_times_service_min_latency_ms_share"), 2).alias("avg_latency_ms_share_times_service_min_latency_ms_share"),
        F.percentile_approx("service_sum_latency_ms_share", [0.25, 0.5, 0.75]).alias("service_sum_latency_ms_share_quartiles"),
        F.round(F.avg("service_min_latency_ms_share"), 2).alias("avg_service_min_latency_ms_share"),
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
    )
    .filter(F.col("avg_service_min_latency_ms_share") < 0)
    .orderBy(F.col("latency_ms_bit_or").desc_nulls_last())
)
