from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("latency_ms")
w2 = Window.partitionBy("service", "attr_key").orderBy(F.col("latency_ms").desc())
w_running = Window.partitionBy("attr_key")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

deduped = (
    lines
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "service", "log_ts", "level", "latency_ms", "status_code", "attr_key")
)

deduped_app_logs = (
    deduped
    .withColumn("row_num", F.rank().over(w2))
    .filter(F.col("row_num") == 1)
)

remaining_keys = (
    deduped_app_logs
    .select("id", "service")
    .exceptAll(
        df_app_logs
        .filter(F.col("service").isin(["checkout", "search"]))
        .select("id", "service")
    )
)

reconciled = (
    deduped_app_logs
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        "row_num",
        F.corr("status_code", "latency_ms").over(w_running).alias("corr_status_code_latency_ms"),
        F.corr("latency_ms", "status_code").over(w_running).alias("corr_latency_ms_status_code"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "level",
        "latency_ms",
        "status_code",
        "row_num",
        "corr_status_code_latency_ms",
    )
    .orderBy(F.col("latency_ms").desc_nulls_first())
)
