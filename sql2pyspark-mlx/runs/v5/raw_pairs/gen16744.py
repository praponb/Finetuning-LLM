from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.orderBy(F.col("latency_ms_str").desc_nulls_first())

lines = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

base = (
    lines
    .filter(F.col("latency_ms").between(1626, 2044))
    .select(
        "id",
        "level",
        "latency_ms",
        "value",
        F.col("latency_ms").cast("double").alias("latency_ms_str"),
        F.coalesce("key", F.lit("OTHER")).alias("key_clean"),
    )
)

valid = (
    base
    .filter(F.col("level").isin(["ERROR", "INFO"]))
    .withColumn("least_latency_ms_str_latency_ms", F.least("latency_ms_str", "latency_ms"))
    .withColumn("latency_ms_str_filled", F.coalesce("latency_ms_str", F.lit(0)))
)

base_app_logs = (
    valid
    .filter(F.col("latency_ms_str_filled") >= 60)
    .select(
        "*",
        F.when(F.col("latency_ms_str_filled") >= 62, "High").when(F.col("latency_ms_str_filled") >= 10, "Medium").otherwise("Low").alias("latency_ms_str_filled_band"),
        F.when(F.col("latency_ms_str_filled") >= 86, "High").when(F.col("latency_ms_str_filled") >= 12, "Medium").otherwise("Low").alias("latency_ms_str_filled_level"),
    )
)

metrics = (
    base_app_logs
    .filter(F.col("latency_ms_str_filled") > 72)
    .withColumn("latency_ms_share", F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4))
)

enriched = (
    metrics
    .select(
        "level",
        "latency_ms",
        "value",
        "key_clean",
        "least_latency_ms_str_latency_ms",
        "latency_ms_str_filled_band",
        "latency_ms_share",
        F.percent_rank().over(w2).alias("latency_ms_str_percent_rank"),
        F.max("latency_ms").over(w).alias("level_max_latency_ms"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w)), 4).alias("latency_ms_share_2"),
    )
)

valid_app_logs = (
    enriched
    .filter(F.col("level_max_latency_ms").isNotNull())
    .select(
        "level",
        "latency_ms",
        "value",
        "key_clean",
        "latency_ms_share",
        "level_max_latency_ms",
        (F.col("latency_ms_share_2") * F.col("latency_ms_share")).alias("latency_ms_share_2_times_latency_ms_share"),
    )
)

stats = (
    valid_app_logs
    .rollup("level")
    .agg(
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
        F.percentile_approx("latency_ms", 0.5).alias("latency_ms_p50"),
        F.count(F.when(F.col("level") == "WARN", 1)).alias("warn_count"),
    )
)

result = stats.select("level", "latency_ms_bit_and", "latency_ms_p50", "warn_count")
