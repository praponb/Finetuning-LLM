from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("attr_key")
w2 = Window.orderBy("attr_key_max_latency_ms")
w3 = Window.partitionBy("service").orderBy(F.col("attr_key_max_latency_ms_rank").desc_nulls_first())
w_ordered = Window.partitionBy("service")
w_grp = Window.orderBy(F.col("latency_ms").asc_nulls_last())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

with_windows = (
    exploded
    .filter(F.col("service") == "checkout")
    .withColumn("attr_key_max_latency_ms", F.max("latency_ms").over(w_running))
)

metrics = (
    with_windows
    .select(
        "service",
        "latency_ms",
        "v",
        F.rank().over(w2).alias("attr_key_max_latency_ms_rank"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w_running)), 4).alias("latency_ms_share"),
    )
)

matched_keys = (
    metrics
    .select("service")
    .subtract(
        df_app_logs
        .filter(F.col("level").isin(["DEBUG", "WARN"]))
        .select("service")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .select(
        "*",
        F.cume_dist().over(w3).alias("attr_key_max_latency_ms_rank_cume_dist"),
        F.min("latency_ms").over(w_ordered).alias("service_min_latency_ms"),
        F.dense_rank().over(w_grp).alias("latency_ms_dense_rank"),
    )
)

remaining_keys = (
    enriched
    .select("service")
    .subtract(
        df_app_logs
        .filter(~F.col("host").isin(["h3", "h2", "h4"]))
        .select("service")
    )
)

verified = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

remaining_keys_2 = (
    verified
    .select("service")
    .subtract(
        df_app_logs
        .filter(F.hour("log_ts").between(8, 12))
        .select("service")
    )
)

kept_rows = (
    verified
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("service", "latency_ms_share", "attr_key_max_latency_ms_rank_cume_dist")
    .unpivot(
        ids=["service"],
        values=["latency_ms_share", "attr_key_max_latency_ms_rank_cume_dist"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
)
