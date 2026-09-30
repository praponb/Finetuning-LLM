from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("status_code").desc(), "id", "k")
w_recent = Window.partitionBy("service", "k").orderBy("status_code", "id", "k")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

deduped = (
    flattened
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

base = (
    deduped
    .filter(F.col("k") != "region")
    .select("*", F.round((F.col("latency_ms") * 0.85), 2).alias("latency_ms_scaled"))
)

with_windows = (
    base
    .select(
        "*",
        F.lead("latency_ms").over(w_recent).alias("next_latency_ms"),
        F.lag("status_code").over(w_recent).alias("prev_status_code"),
        F.row_number().over(w_recent).alias("event_seq"),
    )
)

result = (
    with_windows
    .groupBy(
        "level",
        F.when(F.col("status_code") >= 459, "LARGE").otherwise("SMALL").alias("status_code_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("service")).alias("service_list"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
    )
)
