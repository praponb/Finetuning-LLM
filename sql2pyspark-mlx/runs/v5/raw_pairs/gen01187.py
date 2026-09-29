from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("log_ts", "id", "attr_key")
w2 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("attr_key", "value"),
    )
)

scored = (
    lines
    .filter(~F.col("service").isin(["checkout", "billing"]))
    .select(
        "id",
        "service",
        "attr_key",
        F.lead("latency_ms", 1, 0).over(w).alias("next_latency_ms"),
        F.max("latency_ms").over(w2).alias("latency_ms_max_2h"),
    )
)

cleaned = (
    scored
    .filter(F.col("next_latency_ms") < 992)
    .select(
        "id",
        "service",
        "attr_key",
        "next_latency_ms",
        F.upper("service").alias("service_upper"),
        F.coalesce("attr_key", F.lit("N/A")).alias("attr_key_filled"),
    )
)

filtered = (
    cleaned
    .filter(F.col("attr_key").isNotNull() & ~F.col("attr_key").isin(["version", "region"]))
    .withColumn("next_latency_ms_band", F.when(F.col("next_latency_ms") >= 2045, "High").when(F.col("next_latency_ms") >= 525, "Medium").otherwise("Low"))
    .withColumn("attr_key_clean", F.coalesce("attr_key", F.lit("N/A")))
)

daily = (
    filtered
    .groupBy(
        "attr_key",
        F.when(F.col("next_latency_ms") >= 2682, "LARGE").otherwise("SMALL").alias("next_latency_ms_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.size(F.collect_set("service")).alias("num_services"),
    )
)

result = (
    daily
    .select("attr_key", "next_latency_ms_tier")
    .orderBy(F.col("next_latency_ms_tier").desc_nulls_last())
)
