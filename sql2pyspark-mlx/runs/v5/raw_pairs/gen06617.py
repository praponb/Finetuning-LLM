from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "service").orderBy(F.col("status_code").desc(), "id", "attr_key")
w2 = Window.partitionBy("host")
w3 = Window.partitionBy("attr_key", "host").orderBy(F.col("status_code_rounded").desc_nulls_last())
w4 = Window.partitionBy("attr_key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

deduped = (
    exploded
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

kept_keys = (
    deduped
    .select("host", "service")
    .exceptAll(
        df_app_logs
        .filter(F.to_date("log_ts") < "2023-07-01")
        .select("host", "service")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.host") == F.col("k.host")) & (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("attr_key").isNotNull())
    .select(
        "id",
        "host",
        "latency_ms",
        "attr_key",
        "row_num",
        F.round("status_code", 1).alias("status_code_rounded"),
    )
)

enriched = (
    base
    .withColumn("host_stddev_samp_status_code_rounded", F.stddev_samp("status_code_rounded").over(w2))
    .withColumn("status_code_rounded_rank", F.rank().over(w3))
    .withColumn("attr_key_avg_latency_ms", F.avg("latency_ms").over(w4))
)

result = (
    enriched
    .select(
        "id",
        "host",
        "latency_ms",
        "attr_key",
        "row_num",
        "status_code_rounded",
        "host_stddev_samp_status_code_rounded",
        "status_code_rounded_rank",
        "attr_key_avg_latency_ms",
    )
    .orderBy("status_code_rounded")
)
