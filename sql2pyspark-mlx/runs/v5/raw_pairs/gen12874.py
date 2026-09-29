from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key", "service").orderBy(F.col("latency_ms").desc(), "id", "key")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "v"),
    )
)

base = (
    lines
    .filter(F.col("latency_ms") == 857)
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "key",
        F.when(F.col("key") == "region", 1).otherwise(0).alias("is_region"),
        F.coalesce("key", F.lit("UNKNOWN")).alias("key_filled"),
    )
)

latest = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

kept_keys = (
    latest
    .select("host", "id")
    .exceptAll(
        df_app_logs
        .filter(F.col("service").isin(["search", "auth", "checkout"]))
        .select("host", "id")
    )
)

verified = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.host") == F.col("k.host")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("id", "service", "latency_ms", "key", "row_num")
    .orderBy(F.col("service").desc())
)
