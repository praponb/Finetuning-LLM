from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("attr_key", "service").orderBy("latency_ms", "id", "attr_key")
w2 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy("host").orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("service", "attr_key").orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

enriched = (
    flattened
    .select(
        "*",
        F.ntile(4).over(w_ordered).alias("latency_ms_bucket"),
        F.max("latency_ms").over(w2).alias("latency_ms_max_15m"),
        F.sum("latency_ms").over(w3).alias("cumulative_latency_ms"),
        F.first("attr_key", ignorenulls=True).over(w_part).alias("first_known_attr_key"),
    )
)

matched_keys = (
    enriched
    .select("host", "id")
    .intersect(
        df_app_logs
        .filter(F.to_date("log_ts") < "2024-06-01")
        .select("host", "id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.host") == F.col("k.host")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        "v",
        "latency_ms_max_15m",
        "cumulative_latency_ms",
        F.when(F.col("service").rlike("^(search|checkout)$"), "EXTENDED").otherwise("GROUP_B").alias("service_zone"),
    )
    .orderBy(F.col("cumulative_latency_ms").desc())
)
