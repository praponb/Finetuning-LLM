from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("level").orderBy("latency_ms", "id", "attr_key")
w2 = Window.partitionBy("level").orderBy("latency_ms_filled", "id", "attr_key")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        F.explode("context").alias("attr_key", "value"),
    )
)

metrics = (
    lines
    .select(
        "id",
        "level",
        "latency_ms",
        "attr_key",
        "value",
        F.lag("latency_ms").over(w_running).alias("prev_latency_ms"),
        F.lead("latency_ms").over(w_running).alias("next_latency_ms"),
        F.row_number().over(w_running).alias("seq_no"),
    )
)

valid = (
    metrics
    .filter((~F.col("attr_key").eqNullSafe(F.lit("region"))) & F.col("level").isin(["WARN", "DEBUG", "ERROR"]))
    .select(
        "id",
        "level",
        "attr_key",
        "value",
        "next_latency_ms",
        "seq_no",
        (F.col("next_latency_ms") - F.col("prev_latency_ms")).alias("next_latency_ms_minus_prev_latency_ms"),
        F.coalesce("latency_ms", F.lit(0)).alias("latency_ms_filled"),
    )
)

metrics_app_logs = (
    valid
    .select(
        "*",
        F.lead("latency_ms_filled").over(w2).alias("next_latency_ms_filled"),
        F.lag("latency_ms_filled").over(w2).alias("prev_latency_ms_filled"),
        F.lead("next_latency_ms").over(w2).alias("next_next_latency_ms"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

result = (
    metrics_app_logs
    .select(
        "id",
        "level",
        "attr_key",
        "value",
        "next_latency_ms",
        "prev_latency_ms_filled",
        F.when(F.col("level").rlike("^(ERROR|WARN)$"), "GROUP_A").when(F.col("level").rlike("^(INFO|DEBUG)$"), "CORE").otherwise("EXTENDED").alias("level_zone"),
    )
    .orderBy("next_latency_ms")
)
