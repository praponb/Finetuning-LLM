from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("attr_key").orderBy("latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("host", "attr_key").orderBy("latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

cleaned = (
    flattened
    .filter(F.col("attr_key").isNotNull())
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "attr_key",
        "attr_value",
        F.lower("host").alias("host_lower"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.last("latency_ms", ignorenulls=True).over(w_running).alias("last_known_latency_ms"),
        F.sum("latency_ms").over(w_running).alias("running_latency_ms"),
        F.sum("latency_ms").over(w2).alias("cumulative_latency_ms"),
    )
)

valid = (
    enriched
    .filter(F.col("running_latency_ms") <= 3372)
    .select(
        "*",
        F.col("latency_ms").cast("string").alias("latency_ms_str"),
        F.coalesce("attr_key", F.lit("OTHER")).alias("resolved_attr_key"),
    )
)

result = (
    valid
    .select(
        "id",
        "host",
        "level",
        "attr_key",
        "attr_value",
        "last_known_latency_ms",
        "running_latency_ms",
        "cumulative_latency_ms",
        "latency_ms_str",
    )
    .orderBy(F.col("running_latency_ms").desc_nulls_first(), "level")
)
