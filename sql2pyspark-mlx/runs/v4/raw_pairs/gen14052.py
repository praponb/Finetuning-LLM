from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service", "host").orderBy(F.col("latency_ms_adjusted").asc_nulls_last())
w2 = Window.partitionBy("key").orderBy("latest_rank", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("service").orderBy(F.col("status_code_high").desc(), "id", "key")
w4 = Window.partitionBy("service", "key").orderBy(F.col("latency_ms_adjusted").desc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

flattened_attrs = (
    exploded
    .filter(F.col("service").isin(["billing", "checkout"]))
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("has_fault"),
    )
)

base = (
    flattened_attrs
    .filter(F.col("host").isin(["h3", "h2"]))
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "key",
        "status_code_high",
        F.round((F.col("latency_ms") * 1.07), 2).alias("latency_ms_adjusted"),
        F.coalesce("key", F.lit("OTHER")).alias("key_filled"),
    )
)

latest = (
    base
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    latest
    .withColumn("cumulative_latency_ms_adjusted", F.sum("latency_ms_adjusted").over(w2))
    .withColumn("status_code_high_bucket", F.ntile(3).over(w3))
    .withColumn("latency_ms_adjusted_percent_rank", F.percent_rank().over(w4))
)

totals = (
    enriched
    .groupBy(
        F.when(F.col("latency_ms") >= 2041, "PREMIUM").otherwise("STANDARD").alias("latency_ms_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("latency_ms_adjusted_percent_rank", 0.5).alias("latency_ms_adjusted_percent_rank_p50"),
        F.size(F.collect_set("service")).alias("num_services"),
    )
    .filter(F.col("latency_ms_adjusted_percent_rank_p50") > 0)
)

result = (
    totals
    .select("latency_ms_tier", "cnt", "num_services")
    .orderBy(F.col("latency_ms_tier").desc())
)
